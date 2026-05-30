mod handler;
mod room;

use axum::{
    Router,
    extract::{Path, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::{get, post},
};
use room::RoomRegistry;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

pub type AppState = Arc<RoomRegistry>;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let state: AppState = Arc::new(RoomRegistry::new());

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_headers(Any)
        .allow_methods(Any);

    let app = Router::new()
        .route("/ws/:doc_id",               get(ws_handler))
        .route("/document/:doc_id",         get(handler::get_content))
        .route("/document/:doc_id/ops",     post(handler::apply_ops))    // CRDT real
        .route("/document/:doc_id/text",    post(handler::apply_text))   // fallback
        .route("/document/:doc_id/apply",   post(handler::apply_update)) // Y binário
        .layer(cors)
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:9000").await.unwrap();
    tracing::info!("CRDT server rodando em http://0.0.0.0:9000");
    axum::serve(listener, app).await.unwrap();
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    Path(doc_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    let room = state.get_or_create(doc_id);
    ws.on_upgrade(move |socket| handler::handle_ws(socket, room))
}