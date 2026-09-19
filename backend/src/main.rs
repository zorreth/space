use axum::{
    Router,
    extract::{State, WebSocketUpgrade, ws::WebSocket},
    response::IntoResponse,
    routing::any,
};
use sqlx::{PgPool, postgres::PgPoolOptions};

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
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:8080")
        .await
        .unwrap();

    println!("Listening on 127.0.0.1:8080");
    axum::serve(listener, app).await.unwrap();
}

async fn get_chunks() {}

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
