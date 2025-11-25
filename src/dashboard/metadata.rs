use crate::{
    dashboard::html::{Dir, DirEntry},
    metadata::MetadataClient,
};
use axum::{Router, extract::Path, routing::get};

pub fn router() -> Router<()> {
    let md = MetadataClient::new();

    Router::new()
        .route(
            "/",
            get({
                let md = md.clone();
                async move || {
                    let items = match md.list_scopes().await {
                        Ok(scopes) => Ok(scopes.into_iter().map(|s| DirEntry(s, None)).collect()),
                        Err(e) => Err(format!("Failed to fetch scopes: {e:?}")),
                    };
                    let dir = Dir(
                        "/metadata".to_string(),
                        Some("Known metadata scopes"),
                        items,
                    );
                    dir.render()
                }
            }),
        )
        .route(
            "/{scope}",
            get({
                let md = md.clone();
                async move |Path(scope): Path<String>| {
                    let items = match md.list(scope.clone(), "".to_string()).await {
                        Ok(kvs) => Ok(kvs.into_iter().map(|(k, _v)| DirEntry(k, None)).collect()),
                        Err(e) => Err(format!("Failed to fetch keys: {e:?}")),
                    };
                    let dir = Dir(
                        format!("/metadata/{}", scope),
                        Some("Keys in this metadata scope"),
                        items,
                    );
                    dir.render()
                }
            }),
        )
}
