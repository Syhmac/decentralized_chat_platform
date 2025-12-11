use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State
    },
    response::IntoResponse,
    routing::get,
    Router,
};
use futures::{stream::StreamExt, SinkExt};
use axum::extract::ws::Utf8Bytes;
use tokio::sync::broadcast;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Clone)]
struct ChatMessage {
    username: String,
    content: String,
}

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(stream: WebSocket, state: AppState) {
    let (mut sender, mut reciever) = stream.split();

    let mut rx = state.tx.subscribe();

    //let mut sender_clone = sender;
    tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender
                .send(Message::Text(Utf8Bytes::from(msg.clone())))
                .await
                .is_err() {
                break;
            }
        }
    });

    while let Some(Ok(msg)) = reciever.next().await {
        if let Message::Text(text) = msg {
            if let Ok(chat_msg) = serde_json::from_str::<ChatMessage>(&text) {
                let json = serde_json::to_string(&chat_msg).unwrap();
                let _ = state.tx.send(json);
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let (tx, rx) = broadcast::channel(124);

    let state = AppState { tx };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    let addr = "0.0.0.0:3000";
    println!("Running web socket at {addr}");

    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .expect("server failed to start");
}