use anyhow::Result;
use rusqlite::Connection;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db;
use crate::text::{total_chars, WorkItem};

pub struct PassTracker {
    run_id: String,
    metrics: Vec<PassMetric>,
}

struct PassMetric {
    pass_name: String,
    pages: usize,
    before: usize,
    after: usize,
}

impl PassTracker {
    pub fn new(run_id: String) -> Self {
        PassTracker {
            run_id,
            metrics: Vec::new(),
        }
    }

    pub fn record(&mut self, pass_name: &str, items: &[WorkItem], before: usize) {
        let after = total_chars(items);
        println!(
            "  chars after {}: {} (removed {})",
            pass_name,
            after,
            before.saturating_sub(after)
        );
        self.metrics.push(PassMetric {
            pass_name: pass_name.to_string(),
            pages: items.len(),
            before,
            after,
        });
    }

    pub fn persist(&self, conn: &Connection) -> Result<()> {
        for metric in &self.metrics {
            db::insert_pass_metric(
                conn,
                &self.run_id,
                &metric.pass_name,
                metric.pages,
                metric.before,
                metric.after,
            )?;
        }
        Ok(())
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }
}

pub fn new_run_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("run-{}", now)
}
