use axum::response::Html;

pub struct DirEntry(pub String, pub Option<String>);

pub struct Dir(
    pub String,
    pub Option<&'static str>,
    pub Result<Vec<DirEntry>, String>,
);

const CSS: &'static str = r#"
<style>
    body { font-family: Arial, sans-serif; margin: 2em; }
    h1 { color: #333; }
    ul { list-style-type: none; padding: 0; }
    ul.dir { font-family: monospace; }
    li { margin: 0.5em 0; }
    a { text-decoration: none; color: #0066cc; }
    a:hover { text-decoration: underline; }
</style>"#;

impl Dir {
    pub fn render(self) -> Html<String> {
        let (root, title) = match self.0.as_ref() {
            "/" => ("", "/"),
            s => (s, s),
        };

        let help = match self.1 {
            Some(desc) => format!("<p>{}</p>", desc),
            None => "".to_string(),
        };

        let contents = match self.2 {
            Ok(ents) => {
                let lines = ents
                    .iter()
                    .map(|DirEntry(link, desc)| {
                        let suffix = match desc {
                            Some(s) => format!(" - {}", s),
                            None => "".to_string(),
                        };
                        format!("<li><a href=\"{root}/{0}\">{0}</a>{suffix}</li>", link)
                    })
                    .collect::<Vec<_>>();
                if lines.len() == 0 {
                    "<li><em>(empty)</em></li>".to_string()
                } else {
                    lines.join("\n")
                }
            }
            Err(e) => format!("<li>Error: {}</li>", e),
        };

        Html(format!(
            r#"<!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>Haze: {title}</title>
                {CSS}
            </head>
            <body>
                <h1>{title}</h1>
                {help}
                <ul class="dir">
                <li><a href="../">..</a></li>
                {contents}
                </ul>
            </body>
            </html>"#,
        ))
    }
}
