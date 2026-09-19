use std::collections::HashMap;

use axum::{
    Json, Router,
    extract::{Query, State, WebSocketUpgrade, ws::WebSocket},
    http::StatusCode,
    response::IntoResponse,
    routing::{any, get},
};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};

const MAX_CHUNKS_READ_LIMIT: usize = 20;

#[derive(Clone)]
struct AppState {
    pool: PgPool,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();

    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL should be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
        .unwrap();

    let app_state = AppState { pool };

    let app = Router::new()
        .route("/ws", any(ws_handler))
        .route("/api/chunks", get(get_chunks))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    println!("Listening on 127.0.0.1:8080");
    axum::serve(listener, app).await.unwrap();
}

async fn get_chunks(
    Query(query): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let coords_query = match query.get("coords") {
        Some(coords) => coords,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": "No coords parameter found." })),
            );
        }
    };

    let mut parsed_coords: Vec<i32> = Vec::new();

    for (i, coords) in coords_query.split(';').enumerate() {
        if i + 1 > MAX_CHUNKS_READ_LIMIT {
            break;
        }

        let (x_str, y_str) = match coords.split_once(',') {
            Some(pair) => pair,
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("Invalid coordinate: {}", coords) })),
                );
            }
        };

        if y_str.contains(',') {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": format!("Invalid coordinate: {}", coords) })),
            );
        }

        let x: i32 = match x_str.parse() {
            Ok(x) => x,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("Invalid x coordinate: {}", x_str) })),
                );
            }
        };

        let y: i32 = match y_str.parse() {
            Ok(y) => y,
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({ "error": format!("Invalid y coordinate: {}", y_str) })),
                );
            }
        };

        parsed_coords.push(x);
        parsed_coords.push(y);
    }

    (
        StatusCode::OK,
        Json(json!({ "parsed_coords": parsed_coords })),
    )
}

async fn ws_handler(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| websocket(socket, state))
}

async fn websocket(mut stream: WebSocket, state: AppState) {
    while let Some(msg) = stream.recv().await {
        let msg = if let Ok(msg) = msg {
            msg
        } else {
            return;
        };

        println!("{}", msg.to_text().unwrap());
    }
}
