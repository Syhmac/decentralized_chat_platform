use std::io as std_io;
use std::io::Write;
use std::io::Stdout;
use std::sync::mpsc::{self, Receiver, Sender};

use std::time::Duration;
use time::{OffsetDateTime, UtcOffset};

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
    widgets::{Block, Borders, List, ListItem, Paragraph},
    Terminal,
};

use tokio_tungstenite::connect_async;
use tokio::io::{self, AsyncBufReadExt};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use tokio_tungstenite::tungstenite::Message;

use dcp_commons::utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = "ws://127.0.0.1:3000/ws";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("Connected to server");

    let (mut write, mut read) = ws_stream.split();

    // Getting information about authentication requirement
    let msg = read.next().await.unwrap().unwrap();
    let auth_required: utils::AuthRequired = serde_json::from_str(msg.to_text().unwrap()).unwrap();

    let username: String;
    if auth_required.requires_auth {
        println!("Authentication is required, but not implemented in this client.");
        return Ok(());
    } else { // No authentication - ask user for username
        println!("This server does not support authentication.");
        print!("Enter your username: ");
        Write::flush(&mut std_io::stdout()).unwrap();
        let mut username_mut = String::new();
        std_io::stdin().read_line(&mut username_mut).unwrap();
        username = username_mut.trim().to_string();
    }

    // Channels for communication between UI and WebSocket tasks
    let (tx_ui_in, rx_ui_in): (Sender<String>, Receiver<String>) = mpsc::channel();
    let (tx_out, rx_out): (Sender<String>, Receiver<String>) = mpsc::channel();

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
            let scroll = if total_lines > view_height {
                (total_lines - view_height) as u16
            } else {
                0
            };

            let messages_paragraph = Paragraph::new(joined)
                .block(Block::default().borders(Borders::ALL).title("Messages"))
                .style(Style::default())
                .scroll((0, scroll));
            f.render_widget(messages_paragraph, chunks[0]);

            let input_paragraph = Paragraph::new(input.as_str())
                .block(Block::default().borders(Borders::ALL).title("Input (Enter to send, Esc to quit"))
                .style(Style::default().add_modifier(Modifier::ITALIC));
            f.render_widget(input_paragraph, chunks[1]);
        })?;

        // Poll for keyboard events
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