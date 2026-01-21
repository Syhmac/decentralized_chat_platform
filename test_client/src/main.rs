use std::io as std_io;
use std::io::Write;
use std::sync::mpsc::{self, Receiver, Sender};

use std::time::Duration;
use time::OffsetDateTime;

use crossterm::{
    event::{self, Event as CEvent, KeyCode, KeyEvent},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};

use futures::{SinkExt, StreamExt};

use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout},
    style::{Modifier, Style},
    widgets::{Block, Borders, Paragraph},
    Terminal,
};

use tokio::io::AsyncBufReadExt;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

use dcp_commons::utils;

use std::sync::atomic::{AtomicI64, Ordering};

static OLDEST_MESSAGE_TIMESTAMP: AtomicI64 = AtomicI64::new(0);

fn update_oldest_message_timestamp(ts: i64) {
    let mut current = OLDEST_MESSAGE_TIMESTAMP.load(Ordering::SeqCst);
    loop {
        if current == 0 || ts < current {
            match OLDEST_MESSAGE_TIMESTAMP.compare_exchange(current, ts, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        } else {
            break;
        }
    }
}

fn get_oldest_message_timestamp() -> i64 {
    OLDEST_MESSAGE_TIMESTAMP.load(Ordering::SeqCst)
}

async fn request_unauthenticated_messages(
    write: &mut futures_util::stream::SplitSink<
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        Message,
    >,
    read: &mut futures_util::stream::SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    tx_ui: Sender<String>,
    channel_id: i64,
    authentication: bool,
    timestamp: i64,
    count: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    // Sends a MessageRequest to the server and processes the response.
    
    // Build MessageRequest
    let req = utils::MessageRequest {
        channel_id,
        authentication,
        older_than: timestamp,
        count,
    };
    
    // Build MessageRequest JSON using serde_json
    let req_json = serde_json::to_string(&req)?;

    // Send the request
    if let Err(e) = write.send(Message::Text(Utf8Bytes::from(req_json))).await {
        eprintln!("Failed to send MessageRequest: {}", e);
        return Ok(());
    }

    // Await server response and forward messages to UI in proper order.
    while let Some(Ok(msg)) = read.next().await {
        match msg {
            Message::Text(s) => {
                let text = s.to_string();
                if let Ok(vec) = serde_json::from_str::<Vec<utils::ChatMessageUnAuth>>(&text) {
                    for message in vec {
                        update_oldest_message_timestamp(message.timestamp);
                        let j = serde_json::to_string(&message).unwrap();
                        let _ = tx_ui.send(j);
                    }
                } else if let Ok(message) = serde_json::from_str::<utils::ChatMessageUnAuth>(&text) {
                    update_oldest_message_timestamp(message.timestamp);
                    let j = serde_json::to_string(&message).unwrap();
                    let _ = tx_ui.send(j);
                } else if text == "End stream" {
                    break;
                } else {
                    let _ = tx_ui.send(text);
                }
                break;
            }
            Message::Binary(_) => continue,
            _ => continue,
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "ws://127.0.0.1:3000/ws";
    let mut ws_stream_opt: Option<WebSocketStream<MaybeTlsStream<TcpStream>>> = None;

    while ws_stream_opt.is_none() {
        match connect_async(url).await {
            Ok((stream, _)) => {
                println!("Connected to server at {}", url);
                ws_stream_opt = Some(stream);
            }
            Err(e) => {
                eprintln!("Failed to connect to {}: {}. Retrying in 5 seconds...", url, e);
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    let ws_stream = ws_stream_opt.expect("Failed to establish websocket connection");

    let (mut write, mut read) = ws_stream.split();

    // Getting information about authentication requirement
    let msg = read.next().await.unwrap()?;
    let auth_required: utils::AuthRequired = serde_json::from_str(msg.to_text()?)?;

    let username: String;
    if auth_required.requires_auth {
        println!("Authentication is required, but not implemented in this client.");
        return Ok(());
    } else { // No authentication - ask user for username
        println!("This server does not support authentication.");
        print!("Enter your username: ");
        Write::flush(&mut std_io::stdout())?;
        let mut username_mut = String::new();
        std_io::stdin().read_line(&mut username_mut)?;
        username = username_mut.trim().to_string();
    }

    // Channels for communication between UI and WebSocket tasks
    let (tx_ui_in, rx_ui_in): (Sender<String>, Receiver<String>) = mpsc::channel();
    let (tx_out, rx_out): (Sender<String>, Receiver<String>) = mpsc::channel();

    update_oldest_message_timestamp(utils::get_timestamp(OffsetDateTime::now_utc()));
    // Request initial batch of messages
    request_unauthenticated_messages(
        &mut write,
        &mut read,
        tx_ui_in.clone(),
        0,
        auth_required.requires_auth,
        get_oldest_message_timestamp(),
        5
    ).await?;

    // Handle incoming messages from WebSocket
    let tx_ui_in_clone = tx_ui_in.clone();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = read.next().await {
            match msg {
                Message::Text(s) => {
                    // Send raw text to UI thread
                    let _ = tx_ui_in_clone.send(s.parse().unwrap());
                }
                Message::Binary(_) => {}
                _ => {}
            }
        }
    });

    // Handle outgoing messages to WebSocket
    tokio::spawn(async move {
        while let Ok(text) = rx_out.recv() {
            // Building and serializing ChatMessageUnAuth
            let timestamp = utils::get_timestamp(OffsetDateTime::now_utc());
            let msg = utils::ChatMessageUnAuth {
                username: username.clone(),
                content: text,
                timestamp,
            };
            let json = serde_json::to_string(&msg).unwrap();

            if let Err(e) = write.send(Message::Text(Utf8Bytes::from(json))).await {
                eprintln!("Failed to send message: {}", e);
                break;
            }
        }
    });

    run_ui(rx_ui_in, tx_out)?;

    Ok(())
}

fn run_ui(
    rx_ui_in: Receiver<String>,
    tx_out: Sender<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Terminal setup
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut messages: Vec<String> = Vec::new();
    let mut input = String::new();
    let mut should_quit = false;

    while !should_quit {
        // Drain incoming messages
        while let Ok(msg) = rx_ui_in.try_recv() {
            if let Ok(chat) = serde_json::from_str::<utils::ChatMessageUnAuth>(&msg) {
                messages.push(format!(
                    "[{}] {}:\n{}",
                    utils::get_string_time(chat.timestamp, time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC)),
                    chat.username,
                    chat.content
                ));
            } else if msg == "End stream" {
                // Do nothing
            } else {
                messages.push(msg);
            }
            if messages.len() > 1000 {
                messages.drain(0..(messages.len() - 10000));
            }
        }

        // Draw UI
        terminal.draw(|f| {
            let chunks = Layout::default()
                .direction(Direction::Vertical)
                .margin(1)
                .constraints(
                    [
                        Constraint::Min(1),
                        Constraint::Length(3),
                    ]
                    .as_ref(),
                )
                .split(f.size());

            // Join messages in chronological order
            let joined = messages.join("\n\n");
            let total_lines = joined.lines().count();
            let view_height = chunks[0].height as usize;

            // Scroll to bottom if necessary
            let last_message_lines: usize;
            if messages.is_empty() {
                last_message_lines = 0;
            } else {
                last_message_lines = messages.last().unwrap().lines().count();
            }
            let total_height = total_lines + last_message_lines;
            let scroll = if total_height > view_height {
                (total_lines - view_height + messages[messages.len()-1].lines().count()) as u16
            } else {
                0
            };

            let messages_paragraph = Paragraph::new(joined)
                .block(Block::default().borders(Borders::ALL).title("Messages"))
                .style(Style::default())
                .scroll((scroll, 0));
            f.render_widget(messages_paragraph, chunks[0]);

            let input_paragraph = Paragraph::new(input.as_str())
                .block(Block::default().borders(Borders::ALL).title("Input (Enter to send, Esc to quit"))
                .style(Style::default().add_modifier(Modifier::ITALIC));
            f.render_widget(input_paragraph, chunks[1]);
        })?;

        // Poll for keyboard events
        // FIX: Duplicate event handling in Windows
        if event::poll(Duration::from_millis(100))? {
            if let CEvent::Key(KeyEvent {code, ..}) = event::read()? {
                match code {
                    KeyCode::Char(c) => {input.push(c);}
                    KeyCode::Backspace => {input.pop();}
                    KeyCode::Enter => {
                        let to_send = input.trim().to_string();
                        if !to_send.is_empty() {
                            let _ = tx_out.send(to_send);
                        }
                        input.clear();
                    }
                    KeyCode::Esc => {
                        should_quit = true;
                    }
                    _ => {}
                }
            }
        }
    }

    // Restore terminal
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    Ok(())
}