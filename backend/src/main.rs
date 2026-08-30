use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{State, WebSocketUpgrade, ws::WebSocket},
    response::Response,
    routing::any,
};
use dashmap::DashMap;
use serde::Deserialize;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::broadcast;

const CHUNK_SIZE: i32 = 64;

#[derive(Deserialize, Debug)]
#[serde(tag = "type")]
enum ClientMessage {
    #[serde(rename = "draw")]
    Draw { x: i32, y: i32, color: u32 },

    #[serde(rename = "sub")]
    Sub { chunks: Vec<(i32, i32)> },
}

struct Chunk {
    colors_data: Vec<u8>,
    author_ids_data: Vec<u8>,
    timestamps_data: Vec<u8>,
    last_accessed: u64,
    is_dirty: bool,
}

#[derive(Clone)]
struct AppState {
    pool: PgPool,
    chunks: Arc<DashMap<(i32, i32), Chunk>>,
    rooms: Arc<DashMap<(i32, i32), broadcast::Sender<String>>>,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    let app = Router::new()
        .route("/ws", any(ws_handler))
        .with_state(AppState {
            pool,
            chunks: Arc::new(DashMap::new()),
            rooms: Arc::new(DashMap::new()),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    println!("Listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> Response {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: AppState) {
    println!("Client connected");

    let (tx_outbox, mut rx_outbox) = tokio::sync::mpsc::channel::<String>(100);

    let mut active_subs =
        std::collections::HashMap::<(i32, i32), tokio::task::JoinHandle<()>>::new();

    loop {
        tokio::select! {
            msg = socket.recv() => {
                let raw_msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => {
                        eprintln!("Socket error: {}", e);
                        break;
                    }
                    None => break,
                };

                let text_msg = match raw_msg.to_text() {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("Failed to retrieve text message: {}", e);
                        continue;
                    },
                };

                let message: ClientMessage = match serde_json::from_str(text_msg) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("Failed to parse JSON: {}", e);
                        continue;
                    },
                };

                match message {
                    ClientMessage::Draw { x, y, color } => {
                        let chunk_x = x.div_euclid(CHUNK_SIZE);
                        let chunk_y = y.div_euclid(CHUNK_SIZE);

                        let chunk_coords = (chunk_x, chunk_y);

                        let local_x = x.rem_euclid(CHUNK_SIZE);
                        let local_y = y.rem_euclid(CHUNK_SIZE);

                        let pixel_index = (local_y * CHUNK_SIZE + local_x) as usize;

                        let byte_index = pixel_index * 3;

                        if let Some(mut chunk) = state.chunks.get_mut(&chunk_coords) {
                            let r = ((color >> 16) & 0xFF) as u8;
                            let g = ((color >> 8) & 0xFF) as u8;
                            let b = (color & 0xFF) as u8;

                            chunk.colors_data[byte_index] = r;
                            chunk.colors_data[byte_index + 1] = g;
                            chunk.colors_data[byte_index + 2] = b;

                            chunk.is_dirty = true;
                            chunk.last_accessed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                        }

                        if let Some(room) = state.rooms.get(&chunk_coords) {
                            let out_msg = format!(
                                r#"{{"type":"draw","x":{},"y":{},"color":{}}}"#,
                                x, y, color
                            );

                            room.value().send(out_msg).ok();
                        }
                    },
                    ClientMessage::Sub { mut chunks } => {
                        if chunks.len() > 20 {
                            chunks.truncate(20);
                        }

                        active_subs.retain(|active_chunk, handle| {
                            if chunks.contains(active_chunk) {
                                true
                            } else {
                                handle.abort();
                                false
                            }
                        });

                        for chunk in chunks {
                            if active_subs.contains_key(&chunk) {
                                continue;
                            }

                            let room = state.rooms.entry(chunk).or_insert_with(|| broadcast::channel(100).0);
                            let mut rx = room.value().subscribe();
                            let tx = tx_outbox.clone();

                            let handle = tokio::spawn(async move {
                                loop {
                                    match rx.recv().await {
                                        Ok(result) => {
                                            if tx.send(result).await.is_err() {
                                                break;
                                            }
                                        },
                                        Err(broadcast::error::RecvError::Lagged(_)) => {},
                                        Err(broadcast::error::RecvError::Closed) => {
                                            break;
                                        },
                                    }
                                }
                            });

                            active_subs.insert(chunk, handle);
                        }
                    }
                }
            }

            Some(outbox_msg) = rx_outbox.recv() => {
                if socket.send(axum::extract::ws::Message::Text(outbox_msg.into())).await.is_err() {
                    break;
                }
            }
        }
    }

    println!("Client disconnected");
}
