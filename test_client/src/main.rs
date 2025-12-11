use futures::{SinkExt, StreamExt};
use tokio_tungstenite::connect_async;
use tokio::io::{self, AsyncBufReadExt};
use tokio_tungstenite::tungstenite::Utf8Bytes;

#[tokio::main]
async fn main() {
    let url = "ws://127.0.0.1:3000/ws";
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");

    println!("Connected to server");

    let (mut write, mut read) = ws_stream.split();

    tokio::spawn(async move {
        while let Some(Ok(msg)) = read.next().await {
            println!("Recieved: {:?}", msg);
        }
    });

    let stdin = io::BufReader::new(io::stdin());
    let mut lines = stdin.lines();

    while let Ok(Some(line)) = lines.next_line().await {
        if line.is_empty() {
            break;
        }
        write
            .send(tokio_tungstenite::tungstenite::Message::Text(Utf8Bytes::from(line)))
            .await
            .unwrap();
    }
}
