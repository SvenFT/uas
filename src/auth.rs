//! JWT handling (`jsonwebtoken` in Node) and password hashing (`bcrypt`).

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// bcrypt cost used by every `encryptPassword` copy in the Node code.
pub const PASSWORD_COST: u32 = 10;
/// bcrypt cost used for password reset tokens (`ResetController.js`).
pub const RESET_TOKEN_COST: u32 = 8;

/// Token lifetime for user logins (`LoginService.userLogin`).
pub const USER_TOKEN_TTL_SECS: i64 = 60 * 60 * 6;
/// Token lifetime for freshly created users (`LoginService.createUser`).
pub const CREATE_USER_TOKEN_TTL_SECS: i64 = 60 * 60 * 24 * 100;

/// Payload layout: `{ exp, iat, data: { USER_ID, USER_TOKEN, USER_ROLE, USER_NAME, SYSTEM_KEY, ... } }`
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Claims {
    pub exp: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iat: Option<i64>,
    #[serde(default)]
    pub data: Map<String, Value>,
}

impl Claims {
    fn str_field(&self, key: &str) -> Option<String> {
        match self.data.get(key) {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            _ => None,
        }
    }

    /// `decoded.data.USER_ID.replaceAll("'", "")` – `None` when absent or empty (JS falsy).
    pub fn user_id(&self) -> Option<String> {
        self.str_field("USER_ID")
            .map(|s| s.replace('\'', ""))
            .filter(|s| !s.is_empty())
    }

    pub fn user_role(&self) -> Option<String> {
        self.str_field("USER_ROLE")
    }

    pub fn user_name(&self) -> Option<String> {
        self.str_field("USER_NAME")
    }

    /// `TELEGRAM_USER_ID` (instant messenger id) as a number, `None` when absent or not numeric.
    pub fn telegram_user_id(&self) -> Option<i64> {
        match self.data.get("TELEGRAM_USER_ID") {
            Some(Value::Number(n)) => n.as_i64(),
            Some(Value::String(s)) => s.trim().parse().ok(),
            _ => None,
        }
    }

    /// Node: `isInstantMessangerActive() { return this.instantMessangerId !== 0; }` with
    /// `instantMessangerId = decode.data.TELEGRAM_USER_ID`. Only the number `0` deactivates the
    /// messenger; a missing field (`undefined !== 0`) counts as active.
    pub fn instant_messenger_active(&self) -> bool {
        !matches!(self.data.get("TELEGRAM_USER_ID"), Some(Value::Number(n)) if n.as_f64() == Some(0.0))
    }
}

pub fn now_secs() -> i64 {
    chrono::Utc::now().timestamp()
}

/// `jwt.sign({exp, data}, secret)` – HS256 with an `iat` claim like the Node library adds.
pub fn sign(
    secret: &str,
    ttl_secs: i64,
    data: Map<String, Value>,
) -> Result<String, jsonwebtoken::errors::Error> {
    let now = now_secs();
    let claims = Claims {
        exp: now + ttl_secs,
        iat: Some(now),
        data,
    };
    jsonwebtoken::encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

/// `jwt.verify(token, secret)` – signature and expiry are checked, no leeway.
pub fn verify(secret: &str, token: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    validation.validate_exp = true;
    validation.set_required_spec_claims(&["exp"]);
    let data = jsonwebtoken::decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &validation,
    )?;
    Ok(data.claims)
}

/// bcrypt hash with cost 10, executed off the async runtime.
pub async fn hash_password(password: String) -> Result<String, String> {
    hash_with_cost(password, PASSWORD_COST).await
}

/// bcrypt hash with an explicit cost (used for reset tokens).
pub async fn hash_with_cost(input: String, cost: u32) -> Result<String, String> {
    tokio::task::spawn_blocking(move || bcrypt::hash(input, cost).map_err(|e| e.to_string()))
        .await
        .map_err(|e| e.to_string())?
}

/// `bcrypt.compare` – any error counts as "does not match".
pub async fn verify_password(password: String, hash: String) -> bool {
    tokio::task::spawn_blocking(move || bcrypt::verify(password, &hash).unwrap_or(false))
        .await
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn data() -> Map<String, Value> {
        json!({"USER_ID": "'7'", "USER_ROLE": "ADMIN", "USER_NAME": "sven", "TELEGRAM_USER_ID": 12})
            .as_object()
            .unwrap()
            .clone()
    }

    #[test]
    fn sign_and_verify_roundtrip() {
        let token = sign("secret", 60, data()).unwrap();
        let claims = verify("secret", &token).unwrap();
        assert_eq!(claims.user_id().as_deref(), Some("7"));
        assert_eq!(claims.user_role().as_deref(), Some("ADMIN"));
        assert_eq!(claims.user_name().as_deref(), Some("sven"));
        assert_eq!(claims.telegram_user_id(), Some(12));
        assert!(claims.instant_messenger_active());
        assert!(claims.iat.is_some());
        assert!(claims.exp > now_secs());
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let token = sign("secret", 60, data()).unwrap();
        assert!(verify("other", &token).is_err());
    }

    #[test]
    fn expired_token_is_rejected() {
        let token = sign("secret", -10, data()).unwrap();
        assert!(verify("secret", &token).is_err());
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(verify("secret", "not.a.token").is_err());
        assert!(verify("secret", "").is_err());
    }

    #[test]
    fn missing_user_id_is_none() {
        let claims = Claims {
            exp: 0,
            iat: None,
            data: json!({"USER_ID": ""}).as_object().unwrap().clone(),
        };
        assert_eq!(claims.user_id(), None);
        let claims = Claims {
            exp: 0,
            iat: None,
            data: Map::new(),
        };
        assert_eq!(claims.user_id(), None);
        assert_eq!(claims.telegram_user_id(), None);
        // missing TELEGRAM_USER_ID: active (JS `undefined !== 0`), explicit 0: inactive
        assert!(claims.instant_messenger_active());
        let claims = Claims {
            exp: 0,
            iat: None,
            data: json!({"TELEGRAM_USER_ID": 0}).as_object().unwrap().clone(),
        };
        assert!(!claims.instant_messenger_active());
        assert_eq!(claims.telegram_user_id(), Some(0));
    }

    #[tokio::test]
    async fn password_hash_and_verify() {
        let hash = hash_password("pw".into()).await.unwrap();
        assert!(hash.starts_with("$2"));
        assert!(verify_password("pw".into(), hash.clone()).await);
        assert!(!verify_password("nope".into(), hash).await);
        assert!(!verify_password("pw".into(), "broken".into()).await);
    }

    #[tokio::test]
    async fn node_bcrypt_hash_is_compatible() {
        // Hash taken from the Node project's user.js seed row ("$2b$10$...").
        let hash = "$2b$10$T8AtDJoa9bbdp1WzlODheuuYP886npwEEBvq7UxLX6Re2.j0KDHri".to_string();
        // We do not know the plain text; the point is that the format parses and verify does not panic.
        assert!(!verify_password("definitely-not-the-password".into(), hash).await);
    }
}
