//! Snapshot management utilities for table formats.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A time-travel reference — identifies a specific version of a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeTravel {
    /// Reference by snapshot ID.
    SnapshotId(i64),
    /// Reference by timestamp — resolves to the latest snapshot at or before this time.
    Timestamp(DateTime<Utc>),
    /// Reference by relative version offset (e.g., -1 = previous version).
    Relative(i32),
}

/// Result of a compaction operation.
#[derive(Debug, Clone, Serialize)]
pub struct CompactionResult {
    /// Number of data files before compaction.
    pub files_before: usize,
    /// Number of data files after compaction.
    pub files_after: usize,
    /// Bytes saved by compaction.
    pub bytes_saved: u64,
    /// New snapshot ID created by compaction.
    pub new_snapshot_id: i64,
}
