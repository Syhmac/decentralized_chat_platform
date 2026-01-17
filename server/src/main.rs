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
use dcp_commons::utils;
use sqlite;
use dcp_commons::utils::ChatMessage;

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

async fn handle_socket(mut stream: WebSocket, state: AppState) {
    println!("New WebSocket connection established");

    // Init information exchange
    let auth_required = utils::AuthRequired {
        requires_auth: false,
    };
    stream.send(Message::Text(
        Utf8Bytes::from(serde_json::to_string(&auth_required).unwrap()),
    )).await.expect("Failed to send auth required message");

    // Authenticating user
    if auth_required.requires_auth {
        // Authentication logic would go here (not implemented)
    }

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
            // println!("Received message: {}", text);
            if auth_required.requires_auth {
                // println!("Authentication required, but not implemented");
                // Handle authenticated messages (not implemented)
                continue;
            } else {
                // println!("Authentication not required.");
                if let Ok(chat_msg) = serde_json::from_str::<utils::ChatMessageUnAuth>(&text) {
                    let json = serde_json::to_string(&chat_msg).unwrap();
                    let msg_obj: utils::ChatMessageUnAuth = serde_json::from_str(&json).expect("Failed to parse ChatMessage");
                    let db = sqlite::open("serverData.sqlite").expect("Failed to open database");
                    let query = format!(
                        "INSERT INTO messages_unauthenticated (username, channelID, content, messageTime) VALUES ('{}', {}, '{}', datetime({}, 'unixepoch'));",
                        msg_obj.username, 0, msg_obj.content.replace("'", "''"), msg_obj.timestamp // Using channelID 0 as default - not implemented yet
                    );
                    // println!("Query: {}", query);
                    db.execute(query).expect("Failed to save unauthenticated message to database");
                    // println!("{}", json);
                    let _ = state.tx.send(json);
                }
            }
        }
    }
}

fn prepare_database(db: &sqlite::Connection) {
    println!("Preparing database");
    let mut query = "
        CREATE TABLE IF NOT EXISTS messages (
            messageID INTEGER PRIMARY KEY AUTOINCREMENT,
            userID INTEGER NOT NULL,
            channelID INTEGER NOT NULL,
            content TEXT NOT NULL,
            messageTime DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    ";
    db.execute(query).expect("Failed to create messages table!");
    query = "\
        CREATE TABLE IF NOT EXISTS messages_unauthenticated (\
            unauthenticatedMessageID INTEGER PRIMARY KEY AUTOINCREMENT,\
            username TEXT NOT NULL,\
            channelID INTEGER NOT NULL,\
            content TEXT NOT NULL,\
            messageTime DATETIME DEFAULT CURRENT_TIMESTAMP\
        );\
    ";
    db.execute(query).expect("Failed to create messages_unauthenticated table!");
    query = "
        CREATE TABLE IF NOT EXISTS users (
            userID INTEGER PRIMARY KEY AUTOINCREMENT,
            displayName TEXT NOT NULL,
            passwdHash TEXT NOT NULL
        );
    ";
    db.execute(query).expect("Failed to create users table!");
    query = "
        CREATE TABLE IF NOT EXISTS channels (
            channelID INTEGER PRIMARY KEY AUTOINCREMENT,
            channelName TEXT NOT NULL,
            channelDesc TEXT
        );
    ";
    db.execute(query).expect("Failed to create channels table!");
    query = "
        CREATE TABLE IF NOT EXISTS attachments (
            attachmentID INTEGER PRIMARY KEY AUTOINCREMENT,
            messageID INTEGER NOT NULL,
            type TEXT NOT NULL,
            url TEXT NOT NULL
        );
    ";
    db.execute(query).expect("Failed to create attachments table!");
    println!("Database prepared");
}

#[tokio::main]
async fn main() {
    let (tx, _rx) = broadcast::channel(124);

    let state = AppState { tx };

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .with_state(state);

    println!("Initializing database...");
    let db = sqlite::open("serverData.sqlite").expect("Failed to open database");
    prepare_database(&db);

    let addr = "0.0.0.0:3000";
    println!("Running web socket at {addr}");

    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .expect("server failed to start");
}