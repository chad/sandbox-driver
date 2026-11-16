//! Path resolution the doubles share.

/// The absolute, dot-free path for `path` resolved against `root`.
pub(crate) fn resolve(root: &str, path: &str) -> String {
    let joined = if path.starts_with('/') {
        path.to_owned()
    } else {
        format!("{}/{path}", root.trim_end_matches('/'))
    };
    normalize(&joined)
}

fn normalize(path: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            other => parts.push(other),
        }
    }
    format!("/{}", parts.join("/"))
}
