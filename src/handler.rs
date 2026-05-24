use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use axum::extract::ws::{Message, WebSocket};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use yrs::{GetString, ReadTxn, StateVector, Text, Transact, Update};
use yrs::updates::decoder::Decode;

use crate::room::Room;
use crate::AppState;

// ── Payloads ─────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ApplyPayload {
    pub update: String, // base64 de update Y v2 (usado pelo Laravel)
}

#[derive(Deserialize)]
pub struct TextPayload {
    pub content: String, // texto puro (usado pelo script de teste)
    pub site:    String, // identificador do cliente ex: "maquina-1"
}

#[derive(Serialize, Clone)]
pub struct BroadcastMessage {
    pub content: String,
    pub site:    String,
}

#[derive(Serialize)]
pub struct ContentResponse {
    pub content: String,
    pub state:   String,
}

// ── Função auxiliar: lê conteúdo do Doc de forma síncrona ────────────────
// Usada em get_content e handle_ws para evitar conflito de transações.

fn read_doc(room: &Room) -> (String, Vec<u8>) {
    let doc  = room.doc.blocking_lock();
    let text = doc.get_or_insert_text("content");
    let txn  = doc.transact();
    let content = text.get_string(&txn);
    let state   = txn.encode_state_as_update_v2(&StateVector::default());
    (content, state)
}

// ── REST: GET /document/:doc_id ──────────────────────────────────────────

pub async fn get_content(
    Path(doc_id): Path<String>,
    State(state): State<AppState>,
) -> Json<ContentResponse> {
    let room = state.get_or_create(doc_id);

    let (content, state_bytes) = tokio::task::spawn_blocking(move || {
        read_doc(&room)
    }).await.unwrap_or_default();

    Json(ContentResponse {
        content,
        state: B64.encode(state_bytes),
    })
}

// ── REST: POST /document/:doc_id/text  (modo texto puro — para testes) ───

pub async fn apply_text(
    Path(doc_id): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<TextPayload>,
) -> StatusCode {
    let room    = state.get_or_create(doc_id);
    let content = payload.content.clone();
    let site    = payload.site.clone();

    let content_clone = content.clone();
    let room_clone = room.clone();
    let result = tokio::task::spawn_blocking(move || {
        let doc  = room_clone.doc.blocking_lock();
        let text = doc.get_or_insert_text("content");
        let mut txn = doc.transact_mut();
        let len = text.len(&txn);
        if len > 0 {
            text.remove_range(&mut txn, 0, len);
        }
        text.insert(&mut txn, 0, &content_clone);
        drop(txn);

        let txn   = doc.transact();
        txn.encode_state_as_update_v2(&StateVector::default())
    }).await;

    match result {
        Ok(_state_bytes) => {
            let msg = serde_json::to_vec(&BroadcastMessage { content, site })
                .unwrap_or_default();
            let _ = room.tx.send(msg);
            StatusCode::NO_CONTENT
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ── REST: POST /document/:doc_id/apply  (modo Y update — para o Laravel) ─

fn process_update(room: &Room, payload_b64: &str) -> Result<Vec<u8>, StatusCode> {
    let bytes = B64.decode(payload_b64)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let update = Update::decode_v2(&bytes)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let doc     = room.doc.blocking_lock();
    let mut txn = doc.transact_mut();
    txn.apply_update(update)
        .map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    drop(txn);

    let txn   = doc.transact();
    let state = txn.encode_state_as_update_v2(&StateVector::default());
    Ok(state)
}

pub async fn apply_update(
    Path(doc_id): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<ApplyPayload>,
) -> StatusCode {
    let room       = state.get_or_create(doc_id);
    let room_clone = room.clone();
    let update_b64 = payload.update.clone();

    let result = tokio::task::spawn_blocking(move || {
        process_update(&room_clone, &update_b64)
    }).await;

    match result {
        Ok(Ok(state_bytes)) => {
            let _ = room.tx.send(state_bytes);
            StatusCode::NO_CONTENT
        }
        Ok(Err(status)) => status,
        Err(_)          => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ── WebSocket: /ws/:doc_id ────────────────────────────────────────────────

pub async fn handle_ws(mut socket: WebSocket, room: Arc<Room>) {
    // Estado inicial via spawn_blocking — sem lock async
    let initial_json = tokio::task::spawn_blocking({
        let room = room.clone();
        move || {
            let (content, _) = read_doc(&room);
            serde_json::to_vec(&BroadcastMessage {
                content,
                site: "server".to_string(),
            }).unwrap_or_default()
        }
    }).await.unwrap_or_default();

    if socket.send(Message::Binary(initial_json)).await.is_err() {
        return;
    }

    let mut rx = room.tx.subscribe();

    loop {
        tokio::select! {
            Ok(data) = rx.recv() => {
                if socket.send(Message::Binary(data)).await.is_err() {
                    break;
                }
            }
            msg = socket.next() => {
                if msg.is_none() { break; }
            }
        }
    }
}