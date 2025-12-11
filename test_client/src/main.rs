use futures::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio::io::{self, AsyncBufReadExt};
use tokio_tungstenite::tungstenite::Utf8Bytes;
use serde::{Serialize, Deserialize};
use std::io as std_io;
use std::io::Write;

#[derive(Serialize, Deserialize)]
struct ChatMessage {
    username: String,
    content: String,
}

#[tokio::main]
async fn main() {
    let url = "ws://127.0.0.1:3000/ws";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("Connected to server");

    let (mut write, mut read) = ws_stream.split();

    print!("Enter your username: ");
    Write::flush(&mut std_io::stdout()).unwrap();
    let mut username = String::new();
    std_io::stdin().read_line(&mut username).unwrap();
    let username = username.trim().to_string();

    tokio::spawn(async move {
        while let Some(Ok(msg)) = read.next().await {
            if let tokio_tungstenite::tungstenite::Message::Text(text) = msg {
                if let Ok(chat_msg) = serde_json::from_str::<ChatMessage>(&text) {
                    print!("\x1b[2K\r");
                    std::io::Write::flush(&mut std::io::stdout()).unwrap();

                    println!("{}: {}", chat_msg.username, chat_msg.content);

                    print!("> ");
                    Write::flush(&mut std::io::stdout()).unwrap();
                }
            }
        }
    });

    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    print!("> ");
    Write::flush(&mut std_io::stdout()).unwrap();

    loop {
        if let Ok(Some(line)) = lines.next_line().await {
            if line.is_empty() {
                break;
            }
            let msg = ChatMessage {
                username: username.clone(),
                content: line,
            };
            let json = serde_json::to_string(&msg).unwrap();
            write
                .send(tokio_tungstenite::tungstenite::Message::Text(Utf8Bytes::from(json)))
                .await
                .unwrap();

            print!("\x1b[1A\x1b[2K");
            Write::flush(&mut std_io::stdout()).unwrap();
        }
    }
}
