use std::{borrow::Cow, sync::Arc};

use amimono::rpc::RpcError;
use axum::{http::StatusCode, response::Html};
use futures::future::BoxFuture;

pub type TreeResult<T> = Result<T, TreeError>;

pub enum TreeError {
    NotFound,
    Other(String),
}

impl From<RpcError> for TreeError {
    fn from(value: RpcError) -> Self {
        Self::Other(format!("{value:?}"))
    }
}

pub trait Directory: Send + Sync + Sized + 'static {
    fn list(&self) -> impl Future<Output = TreeResult<Vec<DirEntry>>> + Send;

    fn open_item(&self, name: &str) -> impl Future<Output = TreeResult<Item>> + Send;

    fn open_dir(&self, name: &str) -> impl Future<Output = TreeResult<BoxDirectory>> + Send;

    fn boxed(self) -> BoxDirectory {
        BoxDirectory(Arc::new(DirectoryProxy(self)))
    }
}

pub struct BoxDirectory(Arc<dyn BoxDirectoryTrait>);

impl Clone for BoxDirectory {
    fn clone(&self) -> Self {
        BoxDirectory(self.0.clone())
    }
}

trait BoxDirectoryTrait: Send + Sync + 'static {
    fn list(&'_ self) -> BoxFuture<'_, TreeResult<Vec<DirEntry>>>;

    fn open_item<'d, 'n, 'f>(&'d self, name: &'n str) -> BoxFuture<'f, TreeResult<Item>>
    where
        'd: 'f,
        'n: 'f;

    fn open_dir<'d, 'n, 'f>(&'d self, name: &'n str) -> BoxFuture<'f, TreeResult<BoxDirectory>>
    where
        'd: 'f,
        'n: 'f;
}

struct DirectoryProxy<D: Directory>(D);

impl<D: Directory> BoxDirectoryTrait for DirectoryProxy<D> {
    fn list(&'_ self) -> BoxFuture<'_, TreeResult<Vec<DirEntry>>> {
        Box::pin(self.0.list())
    }

    fn open_item<'d, 'n, 'f>(&'d self, name: &'n str) -> BoxFuture<'f, TreeResult<Item>>
    where
        'd: 'f,
        'n: 'f,
    {
        Box::pin(self.0.open_item(name))
    }

    fn open_dir<'d, 'n, 'f>(&'d self, name: &'n str) -> BoxFuture<'f, TreeResult<BoxDirectory>>
    where
        'd: 'f,
        'n: 'f,
    {
        Box::pin(self.0.open_dir(name))
    }
}

pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

impl DirEntry {
    pub fn item<S: Into<String>>(name: S) -> DirEntry {
        DirEntry {
            name: name.into(),
            is_dir: false,
        }
    }

    pub fn dir<S: Into<String>>(name: S) -> DirEntry {
        DirEntry {
            name: name.into(),
            is_dir: true,
        }
    }
}

pub struct Item {
    pub value: String,
}

impl Item {
    pub fn new<S: Into<String>>(value: S) -> Item {
        Item {
            value: value.into(),
        }
    }
}

impl From<String> for Item {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

fn encode_name(s: &'_ str) -> Cow<'_, str> {
    if s.is_empty() {
        Cow::Borrowed("%00")
    } else if s.contains("/") {
        Cow::Owned(s.replace("/", "%2F"))
    } else {
        Cow::Borrowed(s)
    }
}

fn decode_name(s: &'_ str) -> Cow<'_, str> {
    if s == "%00" {
        Cow::Borrowed("")
    } else if s.contains("%2F") {
        Cow::Owned(s.replace("%2F", "/"))
    } else {
        Cow::Borrowed(s)
    }
}

type TreeResponse = (StatusCode, Html<String>);

pub(crate) async fn render<D: Directory>(dir: D, path: &str) -> TreeResponse {
    let mut cur = dir.boxed();

    let title = path
        .strip_suffix(".html")
        .or(path.strip_suffix("/"))
        .filter(|x| x.is_empty())
        .unwrap_or(path);

    for elem in path.split("/") {
        let elem = decode_name(elem);

        if elem == "" {
            return render_list(cur, title).await;
        } else if let Some(name) = elem.strip_suffix(".html") {
            return render_item(cur, title, name).await;
        } else {
            cur = match cur.0.open_dir(elem.as_ref()).await {
                Ok(next) => next,
                Err(TreeError::NotFound) => return render_404(),
                Err(TreeError::Other(s)) => return render_500(&s),
            }
        }
    }

    render_404()
}

async fn render_list(cur: BoxDirectory, title: &str) -> TreeResponse {
    let (status, contents) = match cur.0.list().await {
        Ok(items) => {
            let contents = items
                .into_iter()
                .map(|it| {
                    let name = encode_name(&it.name);
                    let path = if it.is_dir {
                        format!("{}/", name)
                    } else {
                        format!("{}.html", name)
                    };
                    format!(r#"<li><a href="{path}">{path}</a></li>"#)
                })
                .collect::<Vec<_>>()
                .join("");
            (StatusCode::OK, contents)
        }
        Err(TreeError::NotFound) => {
            return render_404();
        }
        Err(TreeError::Other(s)) => {
            let contents = format!("<li><em>Error: {s}</em></li>");
            (StatusCode::INTERNAL_SERVER_ERROR, contents)
        }
    };

    let page = format!(
        r#"<!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Haze: /{title}</title>
            {CSS}
        </head>
        <body>
            <h1>/{title}</h1>
            <ul class="dir">
            <li><a href="../">..</a></li>
            {contents}
            </ul>
        </body>
        </html>"#
    );

    (status, Html(page))
}

async fn render_item(cur: BoxDirectory, title: &str, name: &str) -> TreeResponse {
    let (status, contents) = match cur.0.open_item(name).await {
        Ok(item) => {
            let clean = item
                .value
                .replace("&", "&amp;")
                .replace("<", "&lt;")
                .replace(">", "&gt;");
            (StatusCode::OK, clean)
        }
        Err(TreeError::NotFound) => {
            return render_404();
        }
        Err(TreeError::Other(s)) => {
            let contents = format!("<em>Error: {s}</em>");
            (StatusCode::INTERNAL_SERVER_ERROR, contents)
        }
    };

    let page = format!(
        r#"<!DOCTYPE html>
        <html>
        <head>
            <meta charset="UTF-8">
            <title>Haze: /{title}</title>
            {CSS}
        </head>
        <body>
            <h1>/{title}</h1>
            <ul class="dir">
            <li><a href="./">Back</a></li>
            </ul>
            <hr />
            <p class="item">{contents}</p>
        </body>
        </html>"#
    );

    (status, Html(page))
}

fn render_404() -> TreeResponse {
    (StatusCode::NOT_FOUND, Html(format!("<p>Not found</p>")))
}

fn render_500(msg: &str) -> TreeResponse {
    let content = format!("<p>Error: {msg}</p>");
    (StatusCode::INTERNAL_SERVER_ERROR, Html(content))
}

const CSS: &'static str = r#"
<style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    h1 { color: #333; }
    ul { list-style-type: none; padding: 0; }
    ul.dir { font-family: monospace; }
    li { margin: 0.5em 0; }
    a { text-decoration: none; color: #0066cc; }
    a:hover { text-decoration: underline; }
    p.item { font-family: monospace; white-space: pre-wrap; }
</style>"#;
