use axum::extract::ws::Utf8Bytes;
use axum::{
    Router,
    extract::{
        State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
    routing::get,
};
use dcp_commons::utils;
use futures::{SinkExt, stream::StreamExt};
use rusqlite::{Connection, params};
use tokio::sync::broadcast;

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<String>,
}

fn prepare_message_bunch(
    db: &Connection,
    channel_id: i64,
    older_than: i64,
    limit: i64,
) -> Vec<String> {
    let mut rows: Vec<String> = Vec::new();
    let sql = "SELECT username, content, messageTime FROM messages_unauthenticated WHERE channelID = ?1 AND messageTime < datetime(?2, 'unixepoch') ORDER BY unauthenticatedMessageID DESC LIMIT ?3;";
    let mut stmt = db.prepare(sql).expect("Failed to prepare statement");
    let mapped = stmt
        .query_map(params![channel_id, older_than, limit], |row| {
            let username: String = row.get(0)?;
            let content: String = row.get(1)?;
            let time: String = row.get(2)?;
            let message_time = time.as_str();
            let message = utils::ChatMessageUnAuth {
                username: username.to_string(),
                content: content.to_string(),
                timestamp: utils::convert_str_time_to_timestamp(
                    message_time,
                    time::UtcOffset::current_local_offset().unwrap_or(time::UtcOffset::UTC),
                )
                .expect("Failed to convert time string to timestamp"),
            };
            let obj = serde_json::to_value(&message).expect("Failed to serialize ChatMessage");
            Ok(obj.to_string())
        })
        .expect("Query execution failed");

    for s in mapped.flatten() {
        rows.push(s);
    }

    rows
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut stream: WebSocket, state: AppState) {
    println!("New WebSocket connection established");

    // Init information exchange
    let auth_required = utils::AuthRequired {
        requires_auth: false,
    };
    stream
        .send(Message::Text(Utf8Bytes::from(
            serde_json::to_string(&auth_required).unwrap(),
        )))
        .await
        .expect("Failed to send auth required message");

    // Authenticating user
    if auth_required.requires_auth {
        // Authentication logic would go here (not implemented)
    }

    let (mut sender, mut reciever) = stream.split();

    let mut rx = state.tx.subscribe();

    // Await request of initial bunch of messages (expecting a utils::MessageRequest as JSON)
    if let Some(Ok(init_msg)) = reciever.next().await
        && let Message::Text(text) = init_msg
        && serde_json::from_str::<utils::MessageRequest>(&text).is_ok()
    {
        let db = Connection::open("serverData.sqlite").expect("Failed to open database");
        // decode utils::MessageRequest
        let msg_request: utils::MessageRequest =
            serde_json::from_str(&text).expect("Failed to parse MessageRequest");
        // Prepare and fetch rows using parameterized query
        let rows: Vec<String> = prepare_message_bunch(
            &db,
            msg_request.channel_id,
            msg_request.older_than,
            msg_request.count as i64,
        );
        // send oldest-first
        if rows.is_empty() {
            if sender
                .send(Message::Text(Utf8Bytes::from("End stream")))
                .await
                .is_err()
            {
                println!("Failed to send end of stream notification");
            }
        } else {
            for json in rows.into_iter().rev() {
                if sender
                    .send(Message::Text(Utf8Bytes::from(json)))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            if sender
                .send(Message::Text(Utf8Bytes::from("End stream")))
                .await
                .is_err()
            {
                println!("Failed to send end of stream notification");
            }
        }
    }

    tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender
                .send(Message::Text(Utf8Bytes::from(msg.clone())))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    while let Some(Ok(msg)) = reciever.next().await {
        if let Message::Text(text) = msg {
            if auth_required.requires_auth {
                continue;
            } else if let Ok(chat_msg) = serde_json::from_str::<utils::ChatMessageUnAuth>(&text) {
                let json = serde_json::to_string(&chat_msg).unwrap();
                let msg_obj: utils::ChatMessageUnAuth =
                    serde_json::from_str(&json).expect("Failed to parse ChatMessage");
                let db = Connection::open("serverData.sqlite").expect("Failed to open database");
                let query = "INSERT INTO messages_unauthenticated (username, channelID, content, messageTime) VALUES (?1, ?2, ?3, datetime(?4, 'unixepoch'));";
                db.execute(
                    query,
                    params![msg_obj.username, 0i64, msg_obj.content, msg_obj.timestamp],
                )
                .expect("Failed to save unauthenticated message to database");
                let _ = state.tx.send(json);
            }
        }
    }
}

fn prepare_database(db: &Connection) {
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
    db.execute(query, [])
        .expect("Failed to create messages table!");
    query = "\
        CREATE TABLE IF NOT EXISTS messages_unauthenticated (\
            unauthenticatedMessageID INTEGER PRIMARY KEY AUTOINCREMENT,\
            username TEXT NOT NULL,\
            channelID INTEGER NOT NULL,\
            content TEXT NOT NULL,\
            messageTime DATETIME DEFAULT CURRENT_TIMESTAMP\
        );\
    ";
    db.execute(query, [])
        .expect("Failed to create messages_unauthenticated table!");
    query = "
        CREATE TABLE IF NOT EXISTS users (
            userID INTEGER PRIMARY KEY AUTOINCREMENT,
            displayName TEXT NOT NULL,
            passwdHash TEXT NOT NULL
        );
    ";
    db.execute(query, [])
        .expect("Failed to create users table!");
    query = "
        CREATE TABLE IF NOT EXISTS channels (
            channelID INTEGER PRIMARY KEY AUTOINCREMENT,
            channelName TEXT NOT NULL,
            channelDesc TEXT
        );
    ";
    db.execute(query, [])
        .expect("Failed to create channels table!");
    query = "
        CREATE TABLE IF NOT EXISTS attachments (
            attachmentID INTEGER PRIMARY KEY AUTOINCREMENT,
            messageID INTEGER NOT NULL,
            type TEXT NOT NULL,
            url TEXT NOT NULL
        );
    ";
    db.execute(query, [])
        .expect("Failed to create attachments table!");
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
    let db = Connection::open("serverData.sqlite").expect("Failed to open database");
    prepare_database(&db);
    db.close().expect("Failed to close database");

    let addr = "0.0.0.0:3000";
    println!("Running web socket at {addr}");

    axum::serve(tokio::net::TcpListener::bind(addr).await.unwrap(), app)
        .await
        .expect("server failed to start");
}
