use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use yrs::Doc;

pub struct Room {
    pub doc: Mutex<Doc>,
    pub tx:  broadcast::Sender<Vec<u8>>,
}

impl Room {
    pub fn new() -> Arc<Self> {
        // Doc::new() já inicializa limpo — não precisa de transação aqui.
        // get_or_insert_text é chamado sob demanda nos handlers.
        let doc = Doc::new();
        let (tx, _) = broadcast::channel(256);
        Arc::new(Self { doc: Mutex::new(doc), tx })
    }
}

pub struct RoomRegistry {
    rooms: DashMap<String, Arc<Room>>,
}

impl RoomRegistry {
    pub fn new() -> Self {
        Self { rooms: DashMap::new() }
    }

    pub fn get_or_create(&self, doc_id: String) -> Arc<Room> {
        self.rooms
            .entry(doc_id)
            .or_insert_with(Room::new)
            .clone()
    }
}