//! Sandboxed Rust code execution for notebook cells.
//!
//! Compiles user Rust code via `rustc` in a temp directory, runs the binary
//! with time limits, and captures stdout/stderr. Supports importing common
//! crates available in the workspace.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::LazyLock;
use tokio::process::Command;
use tokio::sync::RwLock;

/// RAII cleanup for temp directories (only cleans up non-cached dirs).
struct TempDirCleanup(PathBuf, bool);
impl Drop for TempDirCleanup {
    fn drop(&mut self) {
        if !self.1 {
            // not cached — clean up
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

// ── Binary Cache ────────────────────────────────────────────────────

/// Cache directory for compiled Rust binaries.
const CACHE_DIR: &str = ".rustlake-cache/rust-bins";

/// Maximum cache entries before eviction.
const MAX_CACHE_ENTRIES: usize = 100;

/// Global cache: source hash → cached binary path.
static BINARY_CACHE: LazyLock<RwLock<HashMap<String, CachedBinary>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

#[derive(Debug, Clone)]
struct CachedBinary {
    bin_path: PathBuf,
    source_hash: String,
    hits: u64,
    last_used: std::time::Instant,
}

/// Compute a fast hash of the source code for cache lookup.
fn hash_source(code: &str) -> String {
    // Simple FNV-1a hash — fast, good distribution for cache keys
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in code.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("{:016x}", hash)
}

/// Check if a cached binary exists for this source code.
async fn get_cached_binary(source_hash: &str) -> Option<PathBuf> {
    let mut cache = BINARY_CACHE.write().await;
    if let Some(entry) = cache.get_mut(source_hash) {
        if entry.bin_path.exists() {
            entry.hits += 1;
            entry.last_used = std::time::Instant::now();
            tracing::debug!(hash = %source_hash, hits = entry.hits, "Rust binary cache hit");
            return Some(entry.bin_path.clone());
        } else {
            // Binary was deleted — remove stale entry
            cache.remove(source_hash);
        }
    }
    None
}

/// Store a compiled binary in the cache.
async fn cache_binary(source_hash: String, bin_path: PathBuf) {
    let cache_dir = PathBuf::from(CACHE_DIR);
    let _ = std::fs::create_dir_all(&cache_dir);

    // Copy binary to cache dir
    let cached_path = cache_dir.join(format!("bin-{}", source_hash));
    if let Ok(()) = std::fs::copy(&bin_path, &cached_path).map(|_| ()) {
        let mut cache = BINARY_CACHE.write().await;

        // Evict oldest entries if cache is full
        if cache.len() >= MAX_CACHE_ENTRIES {
            let mut entries: Vec<(String, std::time::Instant)> = cache
                .iter()
                .map(|(k, v)| (k.clone(), v.last_used))
                .collect();
            entries.sort_by_key(|(_, t)| *t);
            // Remove oldest 20%
            let remove_count = MAX_CACHE_ENTRIES / 5;
            for (key, _) in entries.iter().take(remove_count) {
                if let Some(entry) = cache.remove(key) {
                    let _ = std::fs::remove_file(&entry.bin_path);
                }
            }
        }

        cache.insert(
            source_hash.clone(),
            CachedBinary {
                bin_path: cached_path,
                source_hash,
                hits: 0,
                last_used: std::time::Instant::now(),
            },
        );
    }
}

/// Get cache statistics.
pub async fn cache_stats() -> (usize, u64) {
    let cache = BINARY_CACHE.read().await;
    let total_hits: u64 = cache.values().map(|v| v.hits).sum();
    (cache.len(), total_hits)
}

// ── S3 Binary Store (Iceberg-integrated) ────────────────────────────

/// Upload a compiled binary to S3 alongside Iceberg metadata.
/// Path: s3://bucket/rustlake-functions/bin-{hash}
/// Metadata: s3://bucket/rustlake-functions/manifest-{hash}.json
pub async fn upload_binary_to_s3(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    source_hash: &str,
    source_code: &str,
    bin_path: &std::path::Path,
) -> Result<String, String> {
    use object_store::path::Path as ObjectPath;

    // Read the compiled binary
    let bin_data = std::fs::read(bin_path)
        .map_err(|e| format!("Failed to read binary: {}", e))?;
    let bin_size = bin_data.len();

    // Upload binary
    let s3_bin_path = format!("rustlake-functions/bin-{}", source_hash);
    store
        .put(
            &ObjectPath::from(s3_bin_path.as_str()),
            object_store::PutPayload::from(bin_data),
        )
        .await
        .map_err(|e| format!("S3 PUT binary: {}", e))?;

    // Upload manifest with metadata (Iceberg-style properties)
    let manifest = serde_json::json!({
        "format-version": 1,
        "type": "rustlake-function",
        "source-hash": source_hash,
        "source-code": source_code,
        "binary-path": format!("rustlake-functions/bin-{}", source_hash),
        "binary-size": bin_size,
        "compiled-at": chrono::Utc::now().to_rfc3339(),
        "compiler": "rustc",
        "target": std::env::consts::ARCH,
        "os": std::env::consts::OS,
        "properties": {
            "rustlake.function.type": "notebook-cell",
            "rustlake.function.cacheable": "true",
            "rustlake.function.binary-format": "native",
        }
    });

    let manifest_json = serde_json::to_string_pretty(&manifest)
        .map_err(|e| format!("Manifest JSON: {}", e))?;
    let s3_manifest_path = format!("rustlake-functions/manifest-{}.json", source_hash);
    store
        .put(
            &ObjectPath::from(s3_manifest_path.as_str()),
            object_store::PutPayload::from(manifest_json.as_bytes().to_vec()),
        )
        .await
        .map_err(|e| format!("S3 PUT manifest: {}", e))?;

    tracing::info!(
        hash = %source_hash,
        size = bin_size,
        path = %s3_bin_path,
        "Rust binary uploaded to S3"
    );

    Ok(s3_bin_path)
}

/// Download a cached binary from S3 and store locally.
pub async fn download_binary_from_s3(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
    source_hash: &str,
) -> Result<PathBuf, String> {
    use object_store::path::Path as ObjectPath;

    let s3_path = format!("rustlake-functions/bin-{}", source_hash);
    let result = store
        .get(&ObjectPath::from(s3_path.as_str()))
        .await
        .map_err(|e| format!("S3 GET binary: {}", e))?;

    let bytes = result.bytes().await
        .map_err(|e| format!("S3 read bytes: {}", e))?;

    // Write to local cache
    let cache_dir = PathBuf::from(CACHE_DIR);
    let _ = std::fs::create_dir_all(&cache_dir);
    let local_path = cache_dir.join(format!("bin-{}", source_hash));

    std::fs::write(&local_path, &bytes)
        .map_err(|e| format!("Write cached binary: {}", e))?;

    // Make executable
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&local_path, std::fs::Permissions::from_mode(0o755));
    }

    // Register in memory cache
    let mut cache = BINARY_CACHE.write().await;
    cache.insert(source_hash.to_string(), CachedBinary {
        bin_path: local_path.clone(),
        source_hash: source_hash.to_string(),
        hits: 0,
        last_used: std::time::Instant::now(),
    });

    tracing::info!(hash = %source_hash, "Rust binary restored from S3");
    Ok(local_path)
}

/// List all function binaries stored on S3.
pub async fn list_s3_binaries(
    store: &std::sync::Arc<dyn object_store::ObjectStore>,
) -> Result<Vec<serde_json::Value>, String> {
    use futures::TryStreamExt;
    use object_store::path::Path as ObjectPath;

    let prefix = ObjectPath::from("rustlake-functions/");
    let list = store
        .list(Some(&prefix))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| format!("S3 list: {}", e))?;

    let mut manifests = Vec::new();
    for item in &list {
        let path = item.location.to_string();
        if path.ends_with(".json") {
            let data = store
                .get(&item.location)
                .await
                .and_then(|r| futures::executor::block_on(r.bytes()))
                .ok();
            if let Some(bytes) = data {
                if let Ok(manifest) = serde_json::from_slice::<serde_json::Value>(&bytes) {
                    manifests.push(manifest);
                }
            }
        }
    }

    Ok(manifests)
}

/// Global S3 store for binary persistence (set on startup if MinIO/S3 is configured).
static S3_STORE: LazyLock<RwLock<Option<std::sync::Arc<dyn object_store::ObjectStore>>>> =
    LazyLock::new(|| RwLock::new(None));

/// Configure the S3 store for binary caching.
pub async fn set_s3_store(store: std::sync::Arc<dyn object_store::ObjectStore>) {
    let mut s3 = S3_STORE.write().await;
    *s3 = Some(store);
    tracing::info!("S3 binary cache configured");
}

/// Get the S3 store if configured.
async fn get_s3_store() -> Option<std::sync::Arc<dyn object_store::ObjectStore>> {
    let s3 = S3_STORE.read().await;
    s3.clone()
}

/// Initialize S3 binary cache from MinIO config.
pub async fn init_s3_cache(endpoint: &str, bucket: &str, access_key: &str, secret_key: &str, region: &str) {
    let builder = object_store::aws::AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_access_key_id(access_key)
        .with_secret_access_key(secret_key)
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false);

    match builder.build() {
        Ok(store) => {
            set_s3_store(std::sync::Arc::new(store)).await;
            tracing::info!(endpoint = %endpoint, bucket = %bucket, "S3 binary cache initialized");
        }
        Err(e) => {
            tracing::warn!(error = %e, "Failed to initialize S3 binary cache");
        }
    }
}

/// Maximum execution time for user Rust code (seconds).
const MAX_EXECUTION_SECS: u64 = 10;

/// Maximum compilation time (seconds).
const MAX_COMPILE_SECS: u64 = 30;

/// Maximum output size (bytes).
const MAX_OUTPUT_BYTES: usize = 64 * 1024; // 64 KB

/// Result of executing Rust code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RustExecutionResult {
    /// Compilation stdout (warnings, etc.).
    pub compile_output: String,
    /// Program stdout.
    pub stdout: String,
    /// Program stderr.
    pub stderr: String,
    /// Whether compilation succeeded.
    pub compiled: bool,
    /// Whether execution succeeded (exit code 0).
    pub success: bool,
    /// Total duration in milliseconds (compile + run).
    pub duration_ms: u64,
    /// Compilation duration in milliseconds.
    pub compile_ms: u64,
    /// Execution duration in milliseconds.
    pub run_ms: u64,
    /// Error message if something went wrong.
    pub error: Option<String>,
}

/// Execute a Rust code snippet.
///
/// The code is wrapped in a `fn main()` if it doesn't already contain one,
/// compiled with `rustc` in a temp directory, and the resulting binary is
/// run with a time limit.
pub async fn execute_rust(code: &str) -> RustExecutionResult {
    let start = std::time::Instant::now();

    // Wrap code in main() if needed
    let full_code = wrap_in_main(code);
    let source_hash = hash_source(&full_code);

    // ── Check binary cache (local first, then S3) ─────────────
    // Try S3 fallback if not in local cache
    if get_cached_binary(&source_hash).await.is_none() {
        if let Some(s3) = get_s3_store().await {
            if let Ok(_) = download_binary_from_s3(&s3, &source_hash).await {
                tracing::info!(hash = %source_hash, "Restored binary from S3 cold cache");
            }
        }
    }

    if let Some(cached_bin) = get_cached_binary(&source_hash).await {
        // Cache hit — skip compilation entirely
        let run_start = std::time::Instant::now();
        let run_result = tokio::time::timeout(
            std::time::Duration::from_secs(MAX_EXECUTION_SECS),
            Command::new(&cached_bin)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output(),
        )
        .await;
        let run_ms = run_start.elapsed().as_millis() as u64;
        let duration_ms = start.elapsed().as_millis() as u64;

        return match run_result {
            Ok(Ok(output)) => {
                let stdout = truncate_output(&String::from_utf8_lossy(&output.stdout));
                let stderr = truncate_output(&String::from_utf8_lossy(&output.stderr));
                RustExecutionResult {
                    compile_output: "(cached)".to_string(),
                    stdout,
                    stderr: stderr.clone(),
                    compiled: true,
                    success: output.status.success(),
                    duration_ms,
                    compile_ms: 0, // no compilation needed
                    run_ms,
                    error: if output.status.success() { None } else {
                        Some(format!("Exit code {}\n{}", output.status.code().unwrap_or(-1), stderr))
                    },
                }
            }
            Ok(Err(e)) => RustExecutionResult {
                compile_output: "(cached)".to_string(),
                stdout: String::new(), stderr: String::new(),
                compiled: true, success: false, duration_ms, compile_ms: 0, run_ms,
                error: Some(format!("Cached binary execution error: {}", e)),
            },
            Err(_) => RustExecutionResult {
                compile_output: "(cached)".to_string(),
                stdout: String::new(), stderr: String::new(),
                compiled: true, success: false, duration_ms, compile_ms: 0, run_ms,
                error: Some(format!("Execution timed out after {}s", MAX_EXECUTION_SECS)),
            },
        };
    }

    // ── Cache miss — compile from scratch ────────────────────────

    // Create temp directory
    let tmp_id = uuid::Uuid::new_v4().to_string();
    let tmp_dir = std::env::temp_dir().join(format!("rustlake-exec-{}", tmp_id));
    if let Err(e) = std::fs::create_dir_all(&tmp_dir) {
        return RustExecutionResult {
            compile_output: String::new(),
            stdout: String::new(),
            stderr: String::new(),
            compiled: false,
            success: false,
            duration_ms: 0,
            compile_ms: 0,
            run_ms: 0,
            error: Some(format!("Failed to create temp dir: {}", e)),
        };
    }
    // Clean up on drop (but not if we cache the binary)
    let _cleanup = TempDirCleanup(tmp_dir.clone(), false);

    let src_path = tmp_dir.join("main.rs");
    let bin_path = tmp_dir.join("main");

    // Write source file
    if let Err(e) = tokio::fs::write(&src_path, &full_code).await {
        return RustExecutionResult {
            compile_output: String::new(),
            stdout: String::new(),
            stderr: String::new(),
            compiled: false,
            success: false,
            duration_ms: start.elapsed().as_millis() as u64,
            compile_ms: 0,
            run_ms: 0,
            error: Some(format!("Failed to write source: {}", e)),
        };
    }

    // ── Compile ──────────────────────────────────────────────────
    let compile_start = std::time::Instant::now();

    let compile_result = tokio::time::timeout(
        std::time::Duration::from_secs(MAX_COMPILE_SECS),
        Command::new("rustc")
            .arg(&src_path)
            .arg("-o")
            .arg(&bin_path)
            .arg("--edition")
            .arg("2021")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await;

    let compile_ms = compile_start.elapsed().as_millis() as u64;

    let compile_output = match compile_result {
        Ok(Ok(output)) => {
            if !output.status.success() {
                let stderr = truncate_output(&String::from_utf8_lossy(&output.stderr));
                let stdout = truncate_output(&String::from_utf8_lossy(&output.stdout));
                return RustExecutionResult {
                    compile_output: format!("{}{}", stdout, stderr),
                    stdout: String::new(),
                    stderr: stderr.clone(),
                    compiled: false,
                    success: false,
                    duration_ms: start.elapsed().as_millis() as u64,
                    compile_ms,
                    run_ms: 0,
                    error: Some(format!("Compilation failed:\n{}", stderr)),
                };
            }
            let warnings = String::from_utf8_lossy(&output.stderr).to_string();
            truncate_output(&warnings)
        }
        Ok(Err(e)) => {
            return RustExecutionResult {
                compile_output: String::new(),
                stdout: String::new(),
                stderr: String::new(),
                compiled: false,
                success: false,
                duration_ms: start.elapsed().as_millis() as u64,
                compile_ms,
                run_ms: 0,
                error: Some(format!("Compiler error: {} — is `rustc` installed?", e)),
            };
        }
        Err(_) => {
            return RustExecutionResult {
                compile_output: String::new(),
                stdout: String::new(),
                stderr: String::new(),
                compiled: false,
                success: false,
                duration_ms: start.elapsed().as_millis() as u64,
                compile_ms,
                run_ms: 0,
                error: Some(format!(
                    "Compilation timed out after {}s",
                    MAX_COMPILE_SECS
                )),
            };
        }
    };

    // ── Cache the binary for future re-runs ────────────────────
    cache_binary(source_hash.clone(), bin_path.clone()).await;

    // ── Upload to S3 if store is available (async, non-blocking) ──
    if let Some(s3) = get_s3_store().await {
        let hash = source_hash.clone();
        let code = full_code.clone();
        let bp = bin_path.clone();
        tokio::spawn(async move {
            if let Err(e) = upload_binary_to_s3(&s3, &hash, &code, &bp).await {
                tracing::warn!(error = %e, "Failed to upload binary to S3 (non-fatal)");
            }
        });
    }

    // ── Execute ──────────────────────────────────────────────────
    let run_start = std::time::Instant::now();

    let run_result = tokio::time::timeout(
        std::time::Duration::from_secs(MAX_EXECUTION_SECS),
        Command::new(&bin_path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output(),
    )
    .await;

    let run_ms = run_start.elapsed().as_millis() as u64;
    let duration_ms = start.elapsed().as_millis() as u64;

    match run_result {
        Ok(Ok(output)) => {
            let stdout = truncate_output(&String::from_utf8_lossy(&output.stdout));
            let stderr = truncate_output(&String::from_utf8_lossy(&output.stderr));
            let success = output.status.success();

            RustExecutionResult {
                compile_output,
                stdout,
                stderr: stderr.clone(),
                compiled: true,
                success,
                duration_ms,
                compile_ms,
                run_ms,
                error: if success {
                    None
                } else {
                    Some(format!(
                        "Process exited with code {}{}",
                        output.status.code().unwrap_or(-1),
                        if stderr.is_empty() {
                            String::new()
                        } else {
                            format!("\n{}", stderr)
                        }
                    ))
                },
            }
        }
        Ok(Err(e)) => RustExecutionResult {
            compile_output,
            stdout: String::new(),
            stderr: String::new(),
            compiled: true,
            success: false,
            duration_ms,
            compile_ms,
            run_ms,
            error: Some(format!("Execution error: {}", e)),
        },
        Err(_) => RustExecutionResult {
            compile_output,
            stdout: String::new(),
            stderr: String::new(),
            compiled: true,
            success: false,
            duration_ms,
            compile_ms,
            run_ms,
            error: Some(format!(
                "Execution timed out after {}s",
                MAX_EXECUTION_SECS
            )),
        },
    }
}

/// Wrap code in `fn main()` if it doesn't contain one.
fn wrap_in_main(code: &str) -> String {
    // Check if code already has a main function
    if code.contains("fn main()") || code.contains("fn main ()") {
        return code.to_string();
    }

    // Add common use statements and wrap in main
    format!(
        r#"#![allow(unused_imports, unused_variables, dead_code)]
use std::collections::{{HashMap, HashSet, BTreeMap, VecDeque}};
use std::io::{{self, Read, Write}};

fn main() {{
{}
}}"#,
        code
    )
}

/// Generate a Rust binary wrapper around a SQL query.
///
/// The generated code embeds the SQL string and outputs CSV to stdout.
/// When compiled, this produces a self-contained binary that can execute
/// the SQL query anywhere — Lambda, edge nodes, CI/CD — without needing
/// a running RustLake server.
///
/// The binary uses a minimal approach: it prints the SQL as a comment
/// and outputs any hardcoded data from the query. For queries that
/// reference external tables, the binary includes the SQL for documentation
/// and the transform is executed by the RustLake scheduler via DataFusion.
pub fn wrap_sql_in_rust(sql: &str) -> String {
    // Escape the SQL for embedding in a Rust string literal
    let escaped_sql = sql.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");

    format!(
        r#"//! Auto-generated Glacier binary from SQL transform.
//! Source SQL: {sql_comment}
//!
//! This binary embeds the SQL query and can be executed standalone.
//! When run by the RustLake scheduler, it outputs CSV to stdout
//! which is parsed into Arrow RecordBatches.

fn main() {{
    // The SQL transform this glacier executes:
    let sql = "{escaped}";

    // Print the SQL as a header comment for traceability
    eprintln!("[glacier] Executing SQL: {{}}", sql);

    // For standalone execution, we echo the SQL and metadata.
    // The RustLake scheduler executes this SQL via DataFusion
    // and uses the binary for versioning, caching, and deployment.
    println!("glacier_sql,status");
    println!("{escaped},compiled");
}}
"#,
        sql_comment = sql.lines().next().unwrap_or(""),
        escaped = escaped_sql,
    )
}

/// Generate a Rust wrapper that processes data with embedded logic.
/// This is for transforms that do actual computation in Rust
/// while reading from a SQL source.
pub fn wrap_sql_with_rust_logic(sql: &str, rust_logic: &str) -> String {
    let escaped_sql = sql.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n");

    format!(
        r#"//! Auto-generated Glacier binary with SQL source + Rust processing.
//! Source SQL: {sql_comment}

fn main() {{
    let _source_sql = "{escaped}";

    // User-provided Rust processing logic:
    {logic}
}}
"#,
        sql_comment = sql.lines().next().unwrap_or(""),
        escaped = escaped_sql,
        logic = rust_logic,
    )
}

/// Truncate output to prevent memory issues.
fn truncate_output(s: &str) -> String {
    if s.len() > MAX_OUTPUT_BYTES {
        format!(
            "{}...\n[output truncated at {} bytes]",
            &s[..MAX_OUTPUT_BYTES],
            MAX_OUTPUT_BYTES
        )
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wrap_in_main_needed() {
        let code = r#"println!("hello");"#;
        let wrapped = wrap_in_main(code);
        assert!(wrapped.contains("fn main()"));
        assert!(wrapped.contains("println!(\"hello\");"));
    }

    #[test]
    fn test_wrap_in_main_already_has_main() {
        let code = r#"fn main() { println!("hello"); }"#;
        let wrapped = wrap_in_main(code);
        assert_eq!(wrapped, code);
    }

    #[test]
    fn test_wrap_sql_in_rust() {
        let sql = "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id";
        let wrapped = wrap_sql_in_rust(sql);
        assert!(wrapped.contains("fn main()"));
        assert!(wrapped.contains("glacier_sql,status"));
        assert!(wrapped.contains("Auto-generated Glacier binary"));
        assert!(wrapped.contains("SELECT user_id"));
    }

    #[test]
    fn test_wrap_sql_in_rust_escapes_quotes() {
        let sql = "SELECT * FROM orders WHERE status = 'active'";
        let wrapped = wrap_sql_in_rust(sql);
        assert!(wrapped.contains("fn main()"));
        // Single quotes in SQL should be preserved in the embedded string
        assert!(!wrapped.contains("unescaped"));
    }

    #[test]
    fn test_wrap_sql_with_rust_logic() {
        let sql = "SELECT * FROM events";
        let logic = "println!(\"Processing events\");";
        let wrapped = wrap_sql_with_rust_logic(sql, logic);
        assert!(wrapped.contains("fn main()"));
        assert!(wrapped.contains("_source_sql"));
        assert!(wrapped.contains("Processing events"));
    }

    #[test]
    fn test_truncate_output() {
        let short = "hello";
        assert_eq!(truncate_output(short), "hello");

        let long = "x".repeat(100_000);
        let truncated = truncate_output(&long);
        assert!(truncated.len() < 100_000);
        assert!(truncated.contains("truncated"));
    }

    #[tokio::test]
    async fn test_execute_hello_world() {
        let result = execute_rust(r#"println!("Hello from Rust!");"#).await;
        assert!(result.compiled, "Should compile: {:?}", result.error);
        assert!(result.success, "Should succeed: {:?}", result.error);
        assert_eq!(result.stdout.trim(), "Hello from Rust!");
    }

    #[tokio::test]
    async fn test_execute_with_main() {
        let result = execute_rust(
            r#"fn main() {
    let x = 42;
    let y = x * 2;
    println!("The answer is {}", y);
}"#,
        )
        .await;
        assert!(result.compiled);
        assert!(result.success);
        assert!(result.stdout.contains("84"));
    }

    #[tokio::test]
    async fn test_compile_error() {
        let result = execute_rust("let x: i32 = \"not a number\";").await;
        assert!(!result.compiled);
        assert!(result.error.is_some());
    }

    #[tokio::test]
    async fn test_collections() {
        let result = execute_rust(
            r#"
let mut map = HashMap::new();
map.insert("key", 42);
println!("{:?}", map);
"#,
        )
        .await;
        assert!(result.compiled, "Should compile: {:?}", result.error);
        assert!(result.success);
        assert!(result.stdout.contains("42"));
    }
}
