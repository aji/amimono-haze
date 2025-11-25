use axum::{Router, routing::get};

use crate::{
    dashboard::html::{Dir, DirEntry},
    dht::controller::DhtClient,
};

pub fn router() -> Router<()> {
    let dht = DhtClient::new();

    Router::new().route(
        "/",
        get({
            let dht = dht.clone();
            async move || {
                let items = match dht.list_scopes().await {
                    Ok(scopes) => Ok(scopes.into_iter().map(|s| DirEntry(s, None)).collect()),
                    Err(e) => Err(format!("Failed to fetch scopes: {e:?}")),
                };
                let dir = Dir("/dht".to_string(), Some("Known DHT scopes"), items);
                dir.render()
            }
        }),
    )
}
