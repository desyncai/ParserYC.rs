mod body;
mod db;
mod meta;
mod shorten;
mod stats;
mod utils;

use anyhow::Result;
use config::Config;
use tracing::info;

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

fn main() -> Result<()> {
    init_tracing();
    let settings = Config::builder()
        .add_source(config::Environment::with_prefix("YC"))
        .build()
        .unwrap_or_default();
    info!(settings_loaded = ?settings, msg = "Starting jobs processor");

    println!("YC Jobs Extraction");
    println!("==================\n");

    println!("Database: {:?}\n", db::path());
    let conn = db::connect()?;
    db::create_tables(&conn)?;

    let jobs = db::fetch_jobs(&conn)?;
    println!("Loaded {} job pages\n", jobs.len());
    if jobs.is_empty() {
        return Ok(());
    }

    println!("Pass 1: shorten text");
    let n = shorten::run(&conn, &jobs)?;
    println!("  -> {} rows\n", n);
    stats::print_length_reduction(&conn)?;

    println!("Pass 2: metadata");
    let n = meta::run(&conn)?;
    println!("  -> {} rows\n", n);

    println!("Pass 3: role body + sections");
    let n = body::run(&conn)?;
    println!("  -> {} rows\n", n);

    println!("Pass 4: stats (markdown)");
    stats::print_stats(&conn)?;

    println!("\nDone.");
    Ok(())
}
