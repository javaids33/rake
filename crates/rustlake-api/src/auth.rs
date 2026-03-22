//! JWT-based Role-Based Access Control (RBAC) for the RustLake platform.
//!
//! Provides user authentication, permission checking, and row/column-level
//! security policies. Uses HMAC-SHA256 signed tokens (base64-encoded JSON
//! claims) for stateless auth without external JWT crate dependencies.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::RwLock;
use std::sync::LazyLock;

// ---------------------------------------------------------------------------
// HMAC helpers (using sha2 directly — no `hmac` crate needed)
// ---------------------------------------------------------------------------

/// Simple HMAC-SHA256 using the standard two-pass construction.
/// Returns 32-byte digest.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::Digest;

    const BLOCK_SIZE: usize = 64;

    // If key is longer than block size, hash it first.
    let key_bytes = if key.len() > BLOCK_SIZE {
        let mut hasher = Sha256::new();
        hasher.update(key);
        let result = hasher.finalize();
        let mut k = [0u8; BLOCK_SIZE];
        k[..32].copy_from_slice(&result);
        k
    } else {
        let mut k = [0u8; BLOCK_SIZE];
        k[..key.len()].copy_from_slice(key);
        k
    };

    // Inner pad
    let mut i_key_pad = [0x36u8; BLOCK_SIZE];
    for (i, b) in key_bytes.iter().enumerate() {
        i_key_pad[i] ^= b;
    }

    // Outer pad
    let mut o_key_pad = [0x5cu8; BLOCK_SIZE];
    for (i, b) in key_bytes.iter().enumerate() {
        o_key_pad[i] ^= b;
    }

    // Inner hash
    let mut inner = Sha256::new();
    inner.update(i_key_pad);
    inner.update(message);
    let inner_result = inner.finalize();

    // Outer hash
    let mut outer = Sha256::new();
    outer.update(o_key_pad);
    outer.update(inner_result);
    let outer_result = outer.finalize();

    let mut out = [0u8; 32];
    out.copy_from_slice(&outer_result);
    out
}

/// URL-safe Base64 encode (no padding).
pub fn base64_url_encode(data: &[u8]) -> String {
    let encoded = base64_encode(data);
    encoded
        .replace('+', "-")
        .replace('/', "_")
        .trim_end_matches('=')
        .to_string()
}

/// URL-safe Base64 decode.
fn base64_url_decode(s: &str) -> Result<Vec<u8>, String> {
    let mut s = s.replace('-', "+").replace('_', "/");
    // Add back padding
    let pad = (4 - s.len() % 4) % 4;
    for _ in 0..pad {
        s.push('=');
    }
    base64_decode(&s)
}

/// Standard Base64 encode.
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity((data.len() + 2) / 3 * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

/// Standard Base64 decode.
fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    fn char_val(c: u8) -> Result<u8, String> {
        match c {
            b'A'..=b'Z' => Ok(c - b'A'),
            b'a'..=b'z' => Ok(c - b'a' + 26),
            b'0'..=b'9' => Ok(c - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            b'=' => Ok(0),
            _ => Err(format!("invalid base64 character: {}", c as char)),
        }
    }

    let bytes = s.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err("invalid base64 length".to_string());
    }

    let mut result = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let a = char_val(chunk[0])?;
        let b = char_val(chunk[1])?;
        let c = char_val(chunk[2])?;
        let d = char_val(chunk[3])?;
        let triple = ((a as u32) << 18) | ((b as u32) << 12) | ((c as u32) << 6) | (d as u32);

        result.push(((triple >> 16) & 0xFF) as u8);
        if chunk[2] != b'=' {
            result.push(((triple >> 8) & 0xFF) as u8);
        }
        if chunk[3] != b'=' {
            result.push((triple & 0xFF) as u8);
        }
    }
    Ok(result)
}

// ---------------------------------------------------------------------------
// Core types
// ---------------------------------------------------------------------------

/// A permission that can be granted to a role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Permission {
    /// Read access to tables matching a glob pattern (e.g., "pg.*", "*").
    ReadTable(String),
    /// Write access to tables matching a glob pattern.
    WriteTable(String),
    /// Permission to execute arbitrary SQL queries.
    ExecuteSql,
    /// Permission to create, modify, and delete CDC/streaming pipelines.
    ManagePipelines,
    /// Permission to create, modify, and delete data source connections.
    ManageConnections,
    /// Full administrative access — implies all other permissions.
    Admin,
}

/// A named role with a set of permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Unique role name (e.g., "analyst", "engineer", "admin").
    pub name: String,
    /// Permissions granted to this role.
    pub permissions: Vec<Permission>,
}

/// A registered user in the system.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Unique user identifier.
    pub id: String,
    /// Login username.
    pub username: String,
    /// Email address.
    pub email: String,
    /// Role names assigned to this user.
    pub roles: Vec<String>,
    /// Account creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// Row-level and column-level security policy for a table pattern.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePolicy {
    /// Glob pattern for tables this policy applies to (e.g., "pg.customers").
    pub table_pattern: String,
    /// Optional SQL predicate injected as a WHERE clause for row filtering.
    /// Use `{username}` as placeholder for the current user.
    pub row_filter_sql: Option<String>,
    /// Column names that should be masked (excluded) from query results.
    pub column_mask: Vec<String>,
}

/// Configuration for the authentication subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Secret key used to sign and verify tokens (HMAC-SHA256).
    pub jwt_secret: String,
    /// Token validity duration in hours.
    pub token_expiry_hours: u64,
    /// Whether authentication is enabled. When false, all requests pass
    /// through as an anonymous admin user.
    pub enabled: bool,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            jwt_secret: "rustlake-default-secret-change-me".to_string(),
            token_expiry_hours: 24,
            enabled: false,
        }
    }
}

/// JWT claims payload — encoded into the token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Claims {
    /// Subject — the user ID.
    pub sub: String,
    /// Username for display / policy variable substitution.
    pub username: String,
    /// Role names the user holds.
    pub roles: Vec<String>,
    /// Expiration time (seconds since UNIX epoch).
    pub exp: u64,
    /// Issued-at time (seconds since UNIX epoch).
    pub iat: u64,
}

// ---------------------------------------------------------------------------
// Global auth state (avoids modifying AppState)
// ---------------------------------------------------------------------------

/// Centralized auth state holding all users, roles, and policies.
#[derive(Debug, Clone)]
pub struct AuthState {
    /// Registered users keyed by user ID.
    pub users: HashMap<String, User>,
    /// Defined roles keyed by role name.
    pub roles: HashMap<String, Role>,
    /// Table-level security policies.
    pub policies: Vec<TablePolicy>,
    /// Auth configuration.
    pub config: AuthConfig,
}

impl Default for AuthState {
    fn default() -> Self {
        // Seed with a default admin role and anonymous user.
        let admin_role = Role {
            name: "admin".to_string(),
            permissions: vec![Permission::Admin],
        };

        let analyst_role = Role {
            name: "analyst".to_string(),
            permissions: vec![
                Permission::ReadTable("*".to_string()),
                Permission::ExecuteSql,
            ],
        };

        let engineer_role = Role {
            name: "engineer".to_string(),
            permissions: vec![
                Permission::ReadTable("*".to_string()),
                Permission::WriteTable("*".to_string()),
                Permission::ExecuteSql,
                Permission::ManagePipelines,
                Permission::ManageConnections,
            ],
        };

        let viewer_role = Role {
            name: "viewer".to_string(),
            permissions: vec![
                Permission::ReadTable("*".to_string()),
            ],
        };

        let anonymous_user = User {
            id: "anonymous".to_string(),
            username: "anonymous".to_string(),
            email: "anonymous@localhost".to_string(),
            roles: vec!["admin".to_string()],
            created_at: Utc::now(),
        };

        let mut users = HashMap::new();
        users.insert(anonymous_user.id.clone(), anonymous_user);

        let mut roles = HashMap::new();
        roles.insert(admin_role.name.clone(), admin_role);
        roles.insert(analyst_role.name.clone(), analyst_role);
        roles.insert(engineer_role.name.clone(), engineer_role);
        roles.insert(viewer_role.name.clone(), viewer_role);

        Self {
            users,
            roles,
            policies: Vec::new(),
            config: AuthConfig::default(),
        }
    }
}

/// Global static auth state — thread-safe via `RwLock`.
static AUTH_STATE: LazyLock<Arc<RwLock<AuthState>>> =
    LazyLock::new(|| Arc::new(RwLock::new(AuthState::default())));

/// Get a reference to the global auth state.
pub fn auth_state() -> &'static Arc<RwLock<AuthState>> {
    &AUTH_STATE
}

// ---------------------------------------------------------------------------
// Token creation and validation
// ---------------------------------------------------------------------------

/// Create a signed token for the given user.
///
/// The token format is: `<base64url(header)>.<base64url(claims)>.<base64url(signature)>`
/// where the header is a fixed `{"alg":"HS256","typ":"JWT"}` and the signature
/// is HMAC-SHA256 over `header.claims`.
pub fn create_token(config: &AuthConfig, user: &User) -> Result<String, String> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system time error: {e}"))?
        .as_secs();

    let claims = Claims {
        sub: user.id.clone(),
        username: user.username.clone(),
        roles: user.roles.clone(),
        exp: now + config.token_expiry_hours * 3600,
        iat: now,
    };

    let header = r#"{"alg":"HS256","typ":"JWT"}"#;
    let header_b64 = base64_url_encode(header.as_bytes());

    let claims_json = serde_json::to_string(&claims)
        .map_err(|e| format!("failed to serialize claims: {e}"))?;
    let claims_b64 = base64_url_encode(claims_json.as_bytes());

    let signing_input = format!("{header_b64}.{claims_b64}");
    let signature = hmac_sha256(config.jwt_secret.as_bytes(), signing_input.as_bytes());
    let sig_b64 = base64_url_encode(&signature);

    Ok(format!("{signing_input}.{sig_b64}"))
}

/// Validate and decode a token, returning the claims on success.
pub fn validate_token(config: &AuthConfig, token: &str) -> Result<Claims, String> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err("invalid token format: expected 3 dot-separated parts".to_string());
    }

    let header_b64 = parts[0];
    let claims_b64 = parts[1];
    let sig_b64 = parts[2];

    // Verify signature
    let signing_input = format!("{header_b64}.{claims_b64}");
    let expected_sig = hmac_sha256(config.jwt_secret.as_bytes(), signing_input.as_bytes());
    let expected_sig_b64 = base64_url_encode(&expected_sig);

    if sig_b64 != expected_sig_b64 {
        return Err("invalid token signature".to_string());
    }

    // Decode claims
    let claims_bytes = base64_url_decode(claims_b64)?;
    let claims_json = String::from_utf8(claims_bytes)
        .map_err(|e| format!("invalid UTF-8 in claims: {e}"))?;
    let claims: Claims = serde_json::from_str(&claims_json)
        .map_err(|e| format!("failed to deserialize claims: {e}"))?;

    // Check expiration
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system time error: {e}"))?
        .as_secs();

    if claims.exp < now {
        return Err("token has expired".to_string());
    }

    Ok(claims)
}

// ---------------------------------------------------------------------------
// Permission checking
// ---------------------------------------------------------------------------

/// Check if a glob-style pattern matches a table name.
/// Supports `*` as a wildcard for any sequence of characters.
fn pattern_matches(pattern: &str, table: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    // Simple glob matching: split on '*' and check sequential containment.
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        // No wildcards — exact match.
        return pattern == table;
    }

    let mut remaining = table;

    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }
        if i == 0 {
            // First segment must be a prefix.
            if !remaining.starts_with(part) {
                return false;
            }
            remaining = &remaining[part.len()..];
        } else if i == parts.len() - 1 {
            // Last segment must be a suffix.
            if !remaining.ends_with(part) {
                return false;
            }
            remaining = "";
        } else {
            // Middle segments must appear in order.
            match remaining.find(part) {
                Some(pos) => remaining = &remaining[pos + part.len()..],
                None => return false,
            }
        }
    }

    true
}

/// Check whether the given claims (via their roles) satisfy a required permission.
///
/// Admin permission implies all other permissions. `ReadTable` / `WriteTable`
/// permissions are matched using glob patterns.
pub fn check_permission(claims: &Claims, required: &Permission, roles: &[Role]) -> bool {
    // Collect all permissions from the user's roles.
    let user_permissions: Vec<&Permission> = claims
        .roles
        .iter()
        .filter_map(|role_name| roles.iter().find(|r| r.name == *role_name))
        .flat_map(|role| role.permissions.iter())
        .collect();

    // Admin implies everything.
    if user_permissions.iter().any(|p| matches!(p, Permission::Admin)) {
        return true;
    }

    match required {
        Permission::ReadTable(table) => {
            user_permissions.iter().any(|p| match p {
                Permission::ReadTable(pattern) => pattern_matches(pattern, table),
                Permission::Admin => true,
                _ => false,
            })
        }
        Permission::WriteTable(table) => {
            user_permissions.iter().any(|p| match p {
                Permission::WriteTable(pattern) => pattern_matches(pattern, table),
                Permission::Admin => true,
                _ => false,
            })
        }
        Permission::ExecuteSql => {
            user_permissions.iter().any(|p| matches!(p, Permission::ExecuteSql))
        }
        Permission::ManagePipelines => {
            user_permissions.iter().any(|p| matches!(p, Permission::ManagePipelines))
        }
        Permission::ManageConnections => {
            user_permissions.iter().any(|p| matches!(p, Permission::ManageConnections))
        }
        Permission::Admin => {
            // Already checked above.
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Row-level and column-level security
// ---------------------------------------------------------------------------

/// Apply row-level security by injecting WHERE clauses from matching policies.
///
/// For each policy whose `table_pattern` matches any table referenced in the
/// SQL, the `row_filter_sql` is appended as an AND condition. The placeholder
/// `{username}` in the filter is replaced with the current user's name.
///
/// This is a best-effort text-based injection. For production use, integrate
/// with DataFusion's logical plan rewriting.
pub fn apply_row_filter(sql: &str, policies: &[TablePolicy], claims: &Claims) -> String {
    // If the user is admin, skip row filtering.
    if claims.roles.iter().any(|r| r == "admin") {
        return sql.to_string();
    }

    let mut result = sql.to_string();

    for policy in policies {
        if let Some(ref filter) = policy.row_filter_sql {
            // Check if the table pattern (or a simplified name) appears in the SQL.
            let table_name = policy.table_pattern.replace('*', "");
            if table_name.is_empty() {
                continue;
            }

            let sql_lower = result.to_lowercase();
            let table_lower = table_name.to_lowercase();

            if sql_lower.contains(&table_lower) {
                let resolved_filter = filter.replace("{username}", &claims.username);

                // Try to inject into an existing WHERE clause, or add a new one.
                if let Some(where_pos) = sql_lower.rfind("where") {
                    let insert_pos = where_pos + 6; // length of "where "
                    result = format!(
                        "{} ({}) AND {}",
                        &result[..insert_pos],
                        resolved_filter,
                        &result[insert_pos..]
                    );
                } else {
                    // Append WHERE before any GROUP BY, ORDER BY, LIMIT, or end.
                    let append_pos = find_clause_boundary(&sql_lower);
                    result = format!(
                        "{} WHERE {}{}",
                        &result[..append_pos],
                        resolved_filter,
                        &result[append_pos..]
                    );
                }
            }
        }
    }

    result
}

/// Find the position where a WHERE clause can be inserted — before GROUP BY,
/// ORDER BY, HAVING, LIMIT, or at end of string.
fn find_clause_boundary(sql_lower: &str) -> usize {
    let boundaries = ["group by", "order by", "having", "limit", "union", "intersect", "except"];
    let mut earliest = sql_lower.len();
    for kw in &boundaries {
        if let Some(pos) = sql_lower.find(kw) {
            if pos < earliest {
                earliest = pos;
            }
        }
    }
    earliest
}

/// Filter out columns that are masked by security policies for the given table.
///
/// Returns only the columns the user is allowed to see. Admin users see all
/// columns.
pub fn filter_columns(
    columns: &[String],
    policies: &[TablePolicy],
    table: &str,
    claims: &Claims,
) -> Vec<String> {
    // Admins see everything.
    if claims.roles.iter().any(|r| r == "admin") {
        return columns.to_vec();
    }

    // Collect all masked columns from applicable policies.
    let masked: Vec<&str> = policies
        .iter()
        .filter(|p| pattern_matches(&p.table_pattern, table))
        .flat_map(|p| p.column_mask.iter().map(|s| s.as_str()))
        .collect();

    columns
        .iter()
        .filter(|col| !masked.contains(&col.as_str()))
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// Axum middleware
// ---------------------------------------------------------------------------

/// Axum middleware that extracts and validates a Bearer token from the
/// `Authorization` header.
///
/// When auth is disabled (the default), all requests pass through with an
/// anonymous admin user injected into request extensions.
///
/// When auth is enabled, a missing or invalid token results in a 401 response.
/// On success, the decoded `Claims` and the corresponding `User` (if found)
/// are inserted into request extensions for downstream handlers.
pub async fn auth_middleware(
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let state = AUTH_STATE.read().await;

    // If auth is disabled, inject anonymous admin and pass through.
    if !state.config.enabled {
        let anonymous = state
            .users
            .get("anonymous")
            .cloned()
            .unwrap_or_else(|| User {
                id: "anonymous".to_string(),
                username: "anonymous".to_string(),
                email: "anonymous@localhost".to_string(),
                roles: vec!["admin".to_string()],
                created_at: Utc::now(),
            });

        let claims = Claims {
            sub: anonymous.id.clone(),
            username: anonymous.username.clone(),
            roles: anonymous.roles.clone(),
            exp: u64::MAX,
            iat: 0,
        };

        drop(state);

        let mut request = request;
        request.extensions_mut().insert(claims);
        request.extensions_mut().insert(anonymous);
        return Ok(next.run(request).await);
    }

    // Auth is enabled — extract Bearer token.
    let auth_header = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let token = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        Some(header) if header.starts_with("bearer ") => &header[7..],
        _ => {
            tracing::warn!("auth: missing or malformed Authorization header");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    let config = state.config.clone();
    let users = state.users.clone();
    let roles_map = state.roles.clone();
    drop(state);

    let claims = match validate_token(&config, token) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!("auth: token validation failed: {e}");
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // Verify the user still exists.
    let user = match users.get(&claims.sub) {
        Some(u) => u.clone(),
        None => {
            tracing::warn!("auth: user '{}' not found in auth state", claims.sub);
            return Err(StatusCode::UNAUTHORIZED);
        }
    };

    // Verify the user's roles still match what's in the token (roles could
    // have been revoked after token issuance).
    let current_roles: Vec<String> = user.roles.clone();
    let valid_roles: Vec<String> = claims
        .roles
        .iter()
        .filter(|r| current_roles.contains(r))
        .cloned()
        .collect();

    if valid_roles.is_empty() {
        tracing::warn!(
            "auth: user '{}' has no valid roles (token roles: {:?}, current: {:?})",
            claims.sub,
            claims.roles,
            current_roles
        );
        return Err(StatusCode::FORBIDDEN);
    }

    // Insert validated claims and user into request extensions.
    let validated_claims = Claims {
        roles: valid_roles,
        ..claims
    };

    let mut request = request;
    request.extensions_mut().insert(validated_claims);
    request.extensions_mut().insert(user);

    Ok(next.run(request).await)
}

// ---------------------------------------------------------------------------
// State management helpers
// ---------------------------------------------------------------------------

/// Register a new user in the auth state. Returns an error if the user ID
/// already exists.
pub async fn register_user(user: User) -> Result<(), String> {
    let mut state = AUTH_STATE.write().await;
    if state.users.contains_key(&user.id) {
        return Err(format!("user '{}' already exists", user.id));
    }
    state.users.insert(user.id.clone(), user);
    Ok(())
}

/// Remove a user by ID. Returns an error if the user does not exist.
pub async fn remove_user(user_id: &str) -> Result<User, String> {
    let mut state = AUTH_STATE.write().await;
    state
        .users
        .remove(user_id)
        .ok_or_else(|| format!("user '{user_id}' not found"))
}

/// Register a new role. Overwrites if a role with the same name exists.
pub async fn register_role(role: Role) {
    let mut state = AUTH_STATE.write().await;
    state.roles.insert(role.name.clone(), role);
}

/// Add a table security policy.
pub async fn add_policy(policy: TablePolicy) {
    let mut state = AUTH_STATE.write().await;
    state.policies.push(policy);
}

/// Update the auth configuration (secret, expiry, enabled flag).
pub async fn update_config(config: AuthConfig) {
    let mut state = AUTH_STATE.write().await;
    state.config = config;
}

/// Get a snapshot of the current auth configuration.
pub async fn get_config() -> AuthConfig {
    let state = AUTH_STATE.read().await;
    state.config.clone()
}

/// List all registered users.
pub async fn list_users() -> Vec<User> {
    let state = AUTH_STATE.read().await;
    state.users.values().cloned().collect()
}

/// List all defined roles.
pub async fn list_roles() -> Vec<Role> {
    let state = AUTH_STATE.read().await;
    state.roles.values().cloned().collect()
}

/// List all table policies.
pub async fn list_policies() -> Vec<TablePolicy> {
    let state = AUTH_STATE.read().await;
    state.policies.clone()
}

/// Create a token for a user by username (convenience wrapper).
pub async fn create_token_for_user(username: &str) -> Result<String, String> {
    let state = AUTH_STATE.read().await;
    let user = state
        .users
        .values()
        .find(|u| u.username == username)
        .cloned()
        .ok_or_else(|| format!("user '{username}' not found"))?;
    let config = state.config.clone();
    drop(state);
    create_token(&config, &user)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> AuthConfig {
        AuthConfig {
            jwt_secret: "test-secret-key-for-unit-tests".to_string(),
            token_expiry_hours: 1,
            enabled: true,
        }
    }

    fn test_user() -> User {
        User {
            id: "user-1".to_string(),
            username: "alice".to_string(),
            email: "alice@example.com".to_string(),
            roles: vec!["analyst".to_string()],
            created_at: Utc::now(),
        }
    }

    fn test_roles() -> Vec<Role> {
        vec![
            Role {
                name: "admin".to_string(),
                permissions: vec![Permission::Admin],
            },
            Role {
                name: "analyst".to_string(),
                permissions: vec![
                    Permission::ReadTable("*".to_string()),
                    Permission::ExecuteSql,
                ],
            },
            Role {
                name: "restricted".to_string(),
                permissions: vec![
                    Permission::ReadTable("public.*".to_string()),
                ],
            },
        ]
    }

    #[test]
    fn test_create_and_validate_token() {
        let config = test_config();
        let user = test_user();

        let token = create_token(&config, &user).expect("token creation failed");
        assert!(token.contains('.'), "token should have dot separators");

        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "token should have 3 parts");

        let claims = validate_token(&config, &token).expect("token validation failed");
        assert_eq!(claims.sub, "user-1");
        assert_eq!(claims.username, "alice");
        assert_eq!(claims.roles, vec!["analyst"]);
    }

    #[test]
    fn test_invalid_signature() {
        let config = test_config();
        let user = test_user();

        let token = create_token(&config, &user).expect("token creation failed");

        let other_config = AuthConfig {
            jwt_secret: "wrong-secret".to_string(),
            ..config
        };

        let result = validate_token(&other_config, &token);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("signature"));
    }

    #[test]
    fn test_expired_token() {
        let config = AuthConfig {
            jwt_secret: "test-secret".to_string(),
            token_expiry_hours: 1,
            enabled: true,
        };
        let user = test_user();

        // Create a token manually with exp in the past
        let claims = Claims {
            sub: user.id.clone(),
            username: user.username.clone(),
            roles: user.roles.clone(),
            iat: 1000,
            exp: 1001, // Far in the past
        };
        let claims_json = serde_json::to_string(&claims).unwrap();
        let header = r#"{"alg":"HS256","typ":"JWT"}"#;
        let header_b64 = crate::auth::base64_url_encode(header.as_bytes());
        let claims_b64 = crate::auth::base64_url_encode(claims_json.as_bytes());
        let signing_input = format!("{header_b64}.{claims_b64}");
        let sig = crate::auth::hmac_sha256(config.jwt_secret.as_bytes(), signing_input.as_bytes());
        let sig_b64 = crate::auth::base64_url_encode(&sig);
        let token = format!("{header_b64}.{claims_b64}.{sig_b64}");

        let result = validate_token(&config, &token);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("expired"));
    }

    #[test]
    fn test_check_permission_admin() {
        let roles = test_roles();
        let claims = Claims {
            sub: "admin-1".to_string(),
            username: "root".to_string(),
            roles: vec!["admin".to_string()],
            exp: u64::MAX,
            iat: 0,
        };

        assert!(check_permission(&claims, &Permission::ExecuteSql, &roles));
        assert!(check_permission(
            &claims,
            &Permission::ReadTable("anything".to_string()),
            &roles
        ));
        assert!(check_permission(
            &claims,
            &Permission::ManagePipelines,
            &roles
        ));
    }

    #[test]
    fn test_check_permission_analyst() {
        let roles = test_roles();
        let claims = Claims {
            sub: "user-1".to_string(),
            username: "alice".to_string(),
            roles: vec!["analyst".to_string()],
            exp: u64::MAX,
            iat: 0,
        };

        assert!(check_permission(
            &claims,
            &Permission::ReadTable("pg.orders".to_string()),
            &roles
        ));
        assert!(check_permission(&claims, &Permission::ExecuteSql, &roles));
        assert!(!check_permission(
            &claims,
            &Permission::ManagePipelines,
            &roles
        ));
        assert!(!check_permission(
            &claims,
            &Permission::ManageConnections,
            &roles
        ));
    }

    #[test]
    fn test_check_permission_restricted_glob() {
        let roles = test_roles();
        let claims = Claims {
            sub: "user-2".to_string(),
            username: "bob".to_string(),
            roles: vec!["restricted".to_string()],
            exp: u64::MAX,
            iat: 0,
        };

        assert!(check_permission(
            &claims,
            &Permission::ReadTable("public.orders".to_string()),
            &roles
        ));
        assert!(!check_permission(
            &claims,
            &Permission::ReadTable("secret.passwords".to_string()),
            &roles
        ));
    }

    #[test]
    fn test_pattern_matching() {
        assert!(pattern_matches("*", "anything"));
        assert!(pattern_matches("pg.*", "pg.orders"));
        assert!(!pattern_matches("pg.*", "mysql.orders"));
        assert!(pattern_matches("*.orders", "pg.orders"));
        assert!(pattern_matches("pg.orders", "pg.orders"));
        assert!(!pattern_matches("pg.orders", "pg.customers"));
    }

    #[test]
    fn test_apply_row_filter() {
        let policies = vec![TablePolicy {
            table_pattern: "pg.orders".to_string(),
            row_filter_sql: Some("region = '{username}'".to_string()),
            column_mask: vec![],
        }];

        let claims = Claims {
            sub: "user-1".to_string(),
            username: "alice".to_string(),
            roles: vec!["analyst".to_string()],
            exp: u64::MAX,
            iat: 0,
        };

        let sql = "SELECT * FROM pg.orders";
        let filtered = apply_row_filter(sql, &policies, &claims);
        assert!(
            filtered.contains("region = 'alice'"),
            "should inject row filter with username: {filtered}"
        );
    }

    #[test]
    fn test_apply_row_filter_admin_bypass() {
        let policies = vec![TablePolicy {
            table_pattern: "pg.orders".to_string(),
            row_filter_sql: Some("region = '{username}'".to_string()),
            column_mask: vec![],
        }];

        let claims = Claims {
            sub: "admin-1".to_string(),
            username: "root".to_string(),
            roles: vec!["admin".to_string()],
            exp: u64::MAX,
            iat: 0,
        };

        let sql = "SELECT * FROM pg.orders";
        let filtered = apply_row_filter(sql, &policies, &claims);
        assert_eq!(filtered, sql, "admin should bypass row filters");
    }

    #[test]
    fn test_filter_columns() {
        let policies = vec![TablePolicy {
            table_pattern: "pg.*".to_string(),
            row_filter_sql: None,
            column_mask: vec!["ssn".to_string(), "salary".to_string()],
        }];

        let columns = vec![
            "id".to_string(),
            "name".to_string(),
            "ssn".to_string(),
            "salary".to_string(),
            "department".to_string(),
        ];

        let claims = Claims {
            sub: "user-1".to_string(),
            username: "alice".to_string(),
            roles: vec!["analyst".to_string()],
            exp: u64::MAX,
            iat: 0,
        };

        let visible = filter_columns(&columns, &policies, "pg.employees", &claims);
        assert_eq!(visible, vec!["id", "name", "department"]);
    }

    #[test]
    fn test_filter_columns_admin_sees_all() {
        let policies = vec![TablePolicy {
            table_pattern: "pg.*".to_string(),
            row_filter_sql: None,
            column_mask: vec!["ssn".to_string()],
        }];

        let columns = vec!["id".to_string(), "ssn".to_string()];

        let claims = Claims {
            sub: "admin-1".to_string(),
            username: "root".to_string(),
            roles: vec!["admin".to_string()],
            exp: u64::MAX,
            iat: 0,
        };

        let visible = filter_columns(&columns, &policies, "pg.employees", &claims);
        assert_eq!(visible, vec!["id", "ssn"]);
    }

    #[test]
    fn test_base64_roundtrip() {
        let data = b"hello world, this is a test of base64 encoding!";
        let encoded = base64_url_encode(data);
        let decoded = base64_url_decode(&encoded).expect("decode failed");
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_hmac_consistency() {
        let key = b"secret-key";
        let msg = b"some message";
        let sig1 = hmac_sha256(key, msg);
        let sig2 = hmac_sha256(key, msg);
        assert_eq!(sig1, sig2, "HMAC should be deterministic");

        let sig3 = hmac_sha256(b"other-key", msg);
        assert_ne!(sig1, sig3, "different keys should produce different signatures");
    }
}
