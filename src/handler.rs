use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
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

// ── Payloads ──────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct ApplyPayload {
    pub update: String,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum TextOp {
    Insert { pos: u32, text: String },
    Delete { pos: u32, len: u32 },
}

#[derive(Deserialize)]
pub struct OpsPayload {
    pub ops:  Vec<TextOp>,
    pub site: String,
}

#[derive(Deserialize)]
pub struct TextPayload {
    pub content: String,
    pub site:    String,
}

#[derive(Serialize, Clone)]
pub struct BroadcastMessage {
    pub content: String,
    pub site:    String,
}

#[derive(Serialize, Default)]
pub struct ContentResponse {
    pub content: String,
    pub state:   String,
}

// ── Auxiliar síncrona: lê conteúdo do Doc ────────────────────────────────

fn read_doc(room: &Room) -> (String, Vec<u8>) {
    let doc     = room.doc.blocking_lock();
    let text    = doc.get_or_insert_text("content");
    let txn     = doc.transact();
    let content = text.get_string(&txn);
    let state   = txn.encode_state_as_update_v2(&StateVector::default());
    (content, state)
}

// ── REST: GET /document/:doc_id ───────────────────────────────────────────

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

// ── REST: POST /document/:doc_id/ops ─────────────────────────────────────
// Aplica operações granulares via yrs (CRDT real).
// Retorna JSON com o conteúdo mergeado para o Laravel fazer broadcast.
//
// Quando dois clientes inserem na mesma posição concorrentemente:
//   - O yrs usa (lamport_clock, client_id) para ordenar deterministicamente
//   - Ambos os textos sobrevivem — nenhum é perdido

pub async fn apply_ops(
    Path(doc_id): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<OpsPayload>,
) -> Response {
    let room       = state.get_or_create(doc_id);
    let ops        = payload.ops;
    let site       = payload.site.clone();
    let room_clone = room.clone();

    let result = tokio::task::spawn_blocking(move || -> Result<String, ()> {
        let doc     = room_clone.doc.blocking_lock();
        let text    = doc.get_or_insert_text("content");
        let mut txn = doc.transact_mut();

        for op in &ops {
            match op {
                TextOp::Insert { pos, text: chunk } => {
                    let len      = text.len(&txn);
                    let safe_pos = (*pos).min(len);
                    text.insert(&mut txn, safe_pos, chunk);
                }
                TextOp::Delete { pos, len } => {
                    let doc_len = text.len(&txn);
                    if *pos < doc_len {
                        let safe_len = (*len).min(doc_len - pos);
                        text.remove_range(&mut txn, *pos, safe_len);
                    }
                }
            }
        }
        drop(txn);

        let txn = doc.transact();
        Ok(text.get_string(&txn))
    }).await;

    match result {
        Ok(Ok(content)) => {
            // Broadcast interno para clientes WebSocket diretos (ex: client.py)
            let msg = serde_json::to_vec(&BroadcastMessage {
                content: content.clone(),
                site:    site.clone(),
            }).unwrap_or_default();
            let _ = room.tx.send(msg);

            // Retorna o conteúdo mergeado — Laravel precisa disso para
            // persistir no banco e fazer broadcast via Reverb
            Json(BroadcastMessage { content, site }).into_response()
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

// ── REST: POST /document/:doc_id/text  (fallback texto puro) ─────────────

pub async fn apply_text(
    Path(doc_id): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<TextPayload>,
) -> StatusCode {
    let room          = state.get_or_create(doc_id);
    let content       = payload.content.clone();
    let site          = payload.site.clone();
    let room_clone    = room.clone();
    let content_clone = content.clone();

    let result = tokio::task::spawn_blocking(move || {
        let doc     = room_clone.doc.blocking_lock();
        let text    = doc.get_or_insert_text("content");
        let mut txn = doc.transact_mut();
        let len     = text.len(&txn);
        if len > 0 { text.remove_range(&mut txn, 0, len); }
        text.insert(&mut txn, 0, &content_clone);
        drop(txn);
        let txn = doc.transact();
        txn.encode_state_as_update_v2(&StateVector::default())
    }).await;

    match result {
        Ok(_) => {
            let msg = serde_json::to_vec(&BroadcastMessage { content, site })
                .unwrap_or_default();
            let _ = room.tx.send(msg);
            StatusCode::NO_CONTENT
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ── REST: POST /document/:doc_id/apply  (Y update binário) ───────────────

fn process_update(room: &Room, payload_b64: &str) -> Result<Vec<u8>, StatusCode> {
    let bytes  = B64.decode(payload_b64).map_err(|_| StatusCode::BAD_REQUEST)?;
    let update = Update::decode_v2(&bytes).map_err(|_| StatusCode::BAD_REQUEST)?;
    let doc    = room.doc.blocking_lock();
    let mut txn = doc.transact_mut();
    txn.apply_update(update).map_err(|_| StatusCode::UNPROCESSABLE_ENTITY)?;
    drop(txn);
    let txn = doc.transact();
    Ok(txn.encode_state_as_update_v2(&StateVector::default()))
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
        Ok(Ok(state_bytes)) => { let _ = room.tx.send(state_bytes); StatusCode::NO_CONTENT }
        Ok(Err(s))          => s,
        Err(_)              => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

// ── WebSocket ─────────────────────────────────────────────────────────────

pub async fn handle_ws(mut socket: WebSocket, room: Arc<Room>) {
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
                if socket.send(Message::Binary(data)).await.is_err() { break; }
            }
            msg = socket.next() => {
                if msg.is_none() { break; }
            }
        }
    }
}