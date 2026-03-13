use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use aes_gcm::aead::rand_core::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use std::collections::HashMap;

const CREDENTIALS_PATH: &str = "credentials.enc";
const NONCE_SIZE: usize = 12;

/// A stored credential entry (plaintext, before encryption).
#[derive(Serialize, Deserialize, Debug)]
struct CredentialEntry {
    id: String,
    #[serde(rename = "type")]
    cred_type: String,
    data: serde_json::Value,
}

/// Encrypted credential store using AES-256-GCM.
pub struct CredentialStore {
    cipher: Aes256Gcm,
}

impl CredentialStore {
    /// Create a new credential store.
    ///
    /// Key derivation:
    /// 1. If `RUSTLAKE_SECRET_KEY` is set, SHA-256 hash it to get 32 bytes
    /// 2. Otherwise, derive from hostname + username (dev mode, logs a warning)
    pub fn new() -> Self {
        let key_bytes: [u8; 32] = match std::env::var("RUSTLAKE_SECRET_KEY") {
            Ok(secret) if !secret.is_empty() => {
                tracing::info!("Using RUSTLAKE_SECRET_KEY for credential encryption");
                let mut hasher = Sha256::new();
                hasher.update(secret.as_bytes());
                hasher.finalize().into()
            }
            _ => {
                tracing::warn!(
                    "RUSTLAKE_SECRET_KEY not set — using machine-derived key for credential encryption. \
                     Set RUSTLAKE_SECRET_KEY for production use."
                );
                let host = hostname::get()
                    .map(|h| h.to_string_lossy().to_string())
                    .unwrap_or_else(|_| "localhost".to_string());
                let username = std::env::var("USER")
                    .or_else(|_| std::env::var("USERNAME"))
                    .unwrap_or_else(|_| "rustlake".to_string());
                let mut hasher = Sha256::new();
                hasher.update(format!("rustlake:{}:{}", host, username).as_bytes());
                hasher.finalize().into()
            }
        };

        let cipher = Aes256Gcm::new(&key_bytes.into());
        Self { cipher }
    }

    /// Encrypt a single entry and return the framed bytes.
    fn encrypt_entry(&self, entry: &CredentialEntry) -> Result<Vec<u8>, String> {
        let plaintext = serde_json::to_vec(entry).map_err(|e| e.to_string())?;
        let mut nonce_bytes = [0u8; NONCE_SIZE];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = self.cipher.encrypt(nonce, plaintext.as_ref())
            .map_err(|e| format!("Encryption failed: {}", e))?;

        // Format: [4-byte length of nonce+ciphertext][nonce][ciphertext]
        let payload_len = (NONCE_SIZE + ciphertext.len()) as u32;
        let mut out = Vec::with_capacity(4 + NONCE_SIZE + ciphertext.len());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Decrypt a single entry from raw bytes. Returns (entry, bytes_consumed).
    fn decrypt_entry(&self, data: &[u8]) -> Result<(CredentialEntry, usize), String> {
        if data.len() < 4 {
            return Err("Too short for length prefix".into());
        }
        let payload_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if data.len() < 4 + payload_len || payload_len < NONCE_SIZE {
            return Err("Invalid payload length".into());
        }
        let nonce = Nonce::from_slice(&data[4..4 + NONCE_SIZE]);
        let ciphertext = &data[4 + NONCE_SIZE..4 + payload_len];
        let plaintext = self.cipher.decrypt(nonce, ciphertext)
            .map_err(|_| "Decryption failed — wrong key or corrupted data".to_string())?;
        let entry: CredentialEntry = serde_json::from_slice(&plaintext)
            .map_err(|e| format!("Invalid credential JSON: {}", e))?;
        Ok((entry, 4 + payload_len))
    }

    /// Load all credentials from the encrypted file.
    fn load_all(&self) -> Vec<CredentialEntry> {
        let data = match std::fs::read(CREDENTIALS_PATH) {
            Ok(d) => d,
            Err(_) => return Vec::new(),
        };
        let mut entries = Vec::new();
        let mut offset = 0;
        while offset < data.len() {
            match self.decrypt_entry(&data[offset..]) {
                Ok((entry, consumed)) => {
                    entries.push(entry);
                    offset += consumed;
                }
                Err(e) => {
                    tracing::warn!(offset, error = %e, "Skipping corrupted credential entry");
                    break;
                }
            }
        }
        entries
    }

    /// Rewrite the entire credentials file with the given entries.
    fn save_all(&self, entries: &[CredentialEntry]) -> Result<(), String> {
        let mut buf = Vec::new();
        for entry in entries {
            buf.extend(self.encrypt_entry(entry)?);
        }
        let tmp_path = format!("{}.tmp", CREDENTIALS_PATH);
        std::fs::write(&tmp_path, &buf).map_err(|e| e.to_string())?;
        std::fs::rename(&tmp_path, CREDENTIALS_PATH).map_err(|e| e.to_string())?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(
                CREDENTIALS_PATH,
                std::fs::Permissions::from_mode(0o600),
            );
        }

        Ok(())
    }

    // ── Public API ──────────────────────────────────────────────────

    /// Store a connection password (encrypted on disk).
    pub fn store_password(&self, conn_id: &str, password: &str) -> Result<(), String> {
        let mut entries = self.load_all();
        entries.retain(|e| !(e.id == conn_id && e.cred_type == "password"));
        entries.push(CredentialEntry {
            id: conn_id.to_string(),
            cred_type: "password".to_string(),
            data: serde_json::json!({ "password": password }),
        });
        self.save_all(&entries)
    }

    /// Remove a stored password.
    pub fn remove_password(&self, conn_id: &str) -> Result<(), String> {
        let mut entries = self.load_all();
        entries.retain(|e| !(e.id == conn_id && e.cred_type == "password"));
        self.save_all(&entries)
    }

    /// Load a stored password for a connection.
    #[allow(dead_code)]
    pub fn load_password(&self, conn_id: &str) -> Option<String> {
        self.load_all().into_iter()
            .find(|e| e.id == conn_id && e.cred_type == "password")
            .and_then(|e| e.data.get("password").and_then(|v| v.as_str()).map(String::from))
    }

    /// Load all stored passwords as a HashMap<conn_id, password>.
    pub fn load_all_passwords(&self) -> HashMap<String, String> {
        self.load_all().into_iter()
            .filter(|e| e.cred_type == "password")
            .filter_map(|e| {
                let pw = e.data.get("password")?.as_str()?.to_string();
                Some((e.id, pw))
            })
            .collect()
    }

    /// Store S3 credentials for a bucket.
    pub fn store_s3_creds(&self, bucket: &str, creds: &crate::state::S3BucketCreds) -> Result<(), String> {
        let mut entries = self.load_all();
        entries.retain(|e| !(e.id == bucket && e.cred_type == "s3"));
        entries.push(CredentialEntry {
            id: bucket.to_string(),
            cred_type: "s3".to_string(),
            data: serde_json::to_value(creds).map_err(|e| e.to_string())?,
        });
        self.save_all(&entries)
    }

    /// Load all stored S3 credentials.
    pub fn load_all_s3_creds(&self) -> HashMap<String, crate::state::S3BucketCreds> {
        self.load_all().into_iter()
            .filter(|e| e.cred_type == "s3")
            .filter_map(|e| {
                let creds: crate::state::S3BucketCreds = serde_json::from_value(e.data).ok()?;
                Some((e.id, creds))
            })
            .collect()
    }
}
