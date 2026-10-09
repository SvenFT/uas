//! Minimal user-agent OS detection, replacing `ua-parser-js` (`os.name`) which the Node
//! server only used to build the `WS.SYSTEM.<node>.<OS>` metric.

/// Returns the OS name using the same labels as `ua-parser-js` for the common platforms.
pub fn os_name(user_agent: &str) -> Option<&'static str> {
    let ua = user_agent;
    if ua.is_empty() {
        return None;
    }
    if ua.contains("iPhone") || ua.contains("iPad") || ua.contains("iPod") {
        return Some("iOS");
    }
    if ua.contains("Android") {
        return Some("Android");
    }
    if ua.contains("Windows Phone") {
        return Some("Windows Phone");
    }
    if ua.contains("Windows") {
        return Some("Windows");
    }
    if ua.contains("CrOS") {
        return Some("Chrome OS");
    }
    if ua.contains("Mac OS X") || ua.contains("Macintosh") {
        return Some("Mac OS");
    }
    if ua.contains("Ubuntu") {
        return Some("Ubuntu");
    }
    if ua.contains("Linux") || ua.contains("X11") {
        return Some("Linux");
    }
    None
}

/// `(os.name || "X").toUpperCase().replace(" ", "")` (JS `replace` removes the first space only).
pub fn system_key(user_agent: &str) -> String {
    let name = os_name(user_agent).unwrap_or("X").to_uppercase();
    name.replacen(' ', "", 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_common_platforms() {
        assert_eq!(
            os_name("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"),
            Some("Windows")
        );
        assert_eq!(
            os_name("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"),
            Some("Mac OS")
        );
        assert_eq!(
            os_name("Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X)"),
            Some("iOS")
        );
        assert_eq!(
            os_name("Mozilla/5.0 (Linux; Android 13; Pixel 7)"),
            Some("Android")
        );
        assert_eq!(os_name("Mozilla/5.0 (X11; Linux x86_64)"), Some("Linux"));
        assert_eq!(os_name("Mozilla/5.0 (X11; CrOS x86_64)"), Some("Chrome OS"));
        assert_eq!(os_name("curl/8.0"), None);
        assert_eq!(os_name(""), None);
    }

    #[test]
    fn system_keys_match_node_metric_names() {
        assert_eq!(
            system_key("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"),
            "MACOS"
        );
        assert_eq!(system_key("Mozilla/5.0 (Windows NT 10.0)"), "WINDOWS");
        assert_eq!(system_key("Mozilla/5.0 (X11; CrOS x86_64)"), "CHROMEOS");
        assert_eq!(system_key(""), "X");
    }
}
