use crate::db;
use crate::utils::contains_word;
use anyhow::Result;
use rusqlite::Connection;
use std::collections::HashMap;

pub fn print_length_reduction(conn: &Connection) -> Result<()> {
    let (raw, shortened) = length_totals(conn)?;
    if raw == 0 {
        println!("  No length stats available.");
        return Ok(());
    }
    println!(
        "  chars: raw={} -> shortened={} (removed {})",
        raw,
        shortened,
        raw.saturating_sub(shortened)
    );
    Ok(())
}

fn length_totals(conn: &Connection) -> Result<(i64, i64)> {
    let mut stmt =
        conn.prepare("SELECT COALESCE(SUM(raw_len),0), COALESCE(SUM(shortened_len),0) FROM job_text_shortened")?;
    let mut rows = stmt.query([])?;
    let mut raw = 0;
    let mut shortened = 0;
    if let Some(row) = rows.next()? {
        raw = row.get(0)?;
        shortened = row.get(1)?;
    }
    Ok((raw, shortened))
}

pub fn print_stats(conn: &Connection) -> Result<()> {
    let meta_rows = db::fetch_meta_rows(conn)?;
    if meta_rows.is_empty() {
        println!("No job_meta rows; skipping stats.");
        return Ok(());
    }
    let text_map = db::fetch_text_map(conn)?;

    let total = meta_rows.len();
    let emoji_count = meta_rows.iter().filter(|m| m.has_emoji).count();
    let emoji_pct = percent(emoji_count, total);

    let emoji_by_bucket = emoji_bucket_counts(&meta_rows);
    let engineering_ids: Vec<i64> = meta_rows
        .iter()
        .filter(|m| m.role_bucket == "Engineering")
        .map(|m| m.job_id)
        .collect();
    let stack_counts = stack_stats(&engineering_ids, &text_map);
    let meme_counts = meme_stats(&engineering_ids, &text_map);
    let (resp_count, req_count) = section_counts(conn)?;

    let markdown = render_markdown(
        total,
        emoji_count,
        emoji_pct,
        &emoji_by_bucket,
        &stack_counts,
        &meme_counts,
        resp_count,
        req_count,
    );

    persist_stats(
        conn,
        total,
        emoji_count,
        emoji_pct,
        &emoji_by_bucket,
        &stack_counts,
        &meme_counts,
        resp_count,
        req_count,
        &markdown,
    )?;

    println!("{markdown}");
    Ok(())
}

fn emoji_bucket_counts(rows: &[db::MetaRow]) -> Vec<(String, usize, f64)> {
    let emoji_rows: Vec<&db::MetaRow> = rows.iter().filter(|m| m.has_emoji).collect();
    let total = emoji_rows.len();
    let buckets = ["Sales", "Marketing", "Operations"];

    buckets
        .iter()
        .map(|b| {
            let count = emoji_rows.iter().filter(|m| m.role_bucket == *b).count();
            (b.to_string(), count, percent(count, total))
        })
        .collect()
}

fn stack_stats(engineering_ids: &[i64], text_map: &HashMap<i64, String>) -> Vec<(String, usize)> {
    let keywords: Vec<(&str, Vec<&str>)> = vec![
        ("python", vec!["python"]),
        ("js", vec!["javascript", "typescript", "node", "react"]),
        ("rust", vec!["rust"]),
        ("assembly", vec!["assembly", "asm"]),
        ("go", vec!["golang", "go"]),
        ("scala", vec!["scala"]),
        ("redis", vec!["redis"]),
        ("postgres", vec!["postgres", "postgresql"]),
        ("pytorch", vec!["pytorch"]),
    ];

    keywords
        .into_iter()
        .map(|(label, needles)| {
            let count = engineering_ids
                .iter()
                .filter_map(|id| text_map.get(id))
                .filter(|text| contains_any(text, &needles, label == "go"))
                .count();
            (label.to_string(), count)
        })
        .collect()
}

fn meme_stats(engineering_ids: &[i64], text_map: &HashMap<i64, String>) -> Vec<(String, usize)> {
    let keywords: Vec<(&str, Vec<&str>)> = vec![
        (
            "prompt engineer",
            vec!["prompt engineer", "prompt engineering"],
        ),
        ("openai", vec!["openai", "open ai"]),
        ("gpt/llm", vec!["gpt", "llm"]),
        (
            "vibe coding",
            vec!["vibe coding", "vibe coder", "vibe engineer"],
        ),
    ];

    keywords
        .into_iter()
        .map(|(label, needles)| {
            let count = engineering_ids
                .iter()
                .filter_map(|id| text_map.get(id))
                .filter(|text| contains_any(text, &needles, false))
                .count();
            (label.to_string(), count)
        })
        .collect()
}

fn render_markdown(
    total: usize,
    emoji_count: usize,
    emoji_pct: f64,
    emoji_by_bucket: &[(String, usize, f64)],
    stack_counts: &[(String, usize)],
    meme_counts: &[(String, usize)],
    resp_count: usize,
    req_count: usize,
) -> String {
    let mut out = String::new();
    out.push_str("## Job Stats\n");
    out.push_str(&format!(
        "- Total jobs with metadata: {}\n- Jobs with emojis: {} ({:.1}%)\n",
        total, emoji_count, emoji_pct
    ));
    out.push_str(&format!(
        "- Jobs with responsibilities extracted: {}\n- Jobs with requirements extracted: {}\n",
        resp_count, req_count
    ));

    out.push_str("\n### Emojis by bucket (emoji jobs only)\n");
    for (bucket, count, pct) in emoji_by_bucket {
        out.push_str(&format!("- {}: {} ({:.1}%)\n", bucket, count, pct));
    }

    out.push_str("\n### Engineering stack mentions\n");
    for (label, count) in stack_counts {
        out.push_str(&format!("- {}: {}\n", label, count));
    }

    out.push_str("\n### Engineering meme keywords\n");
    for (label, count) in meme_counts {
        out.push_str(&format!("- {}: {}\n", label, count));
    }

    out
}

fn persist_stats(
    conn: &Connection,
    total: usize,
    emoji_count: usize,
    emoji_pct: f64,
    emoji_by_bucket: &[(String, usize, f64)],
    stack_counts: &[(String, usize)],
    meme_counts: &[(String, usize)],
    resp_count: usize,
    req_count: usize,
    markdown: &str,
) -> Result<()> {
    conn.execute("DELETE FROM job_stats", [])?;
    let mut stmt =
        conn.prepare("INSERT OR REPLACE INTO job_stats (metric, value) VALUES (?, ?)")?;

    stmt.execute(rusqlite::params!["total_jobs", total.to_string()])?;
    stmt.execute(rusqlite::params!["emoji_jobs", emoji_count.to_string()])?;
    stmt.execute(rusqlite::params!["emoji_pct", format!("{:.2}", emoji_pct)])?;
    stmt.execute(rusqlite::params!["has_responsibilities", resp_count.to_string()])?;
    stmt.execute(rusqlite::params!["has_requirements", req_count.to_string()])?;
    for (bucket, count, pct) in emoji_by_bucket {
        stmt.execute(rusqlite::params![
            format!("emoji_bucket:{}:count", bucket),
            count.to_string()
        ])?;
        stmt.execute(rusqlite::params![
            format!("emoji_bucket:{}:pct", bucket),
            format!("{:.2}", pct)
        ])?;
    }
    for (label, count) in stack_counts {
        stmt.execute(rusqlite::params![
            format!("stack:{}", label),
            count.to_string()
        ])?;
    }
    for (label, count) in meme_counts {
        stmt.execute(rusqlite::params![
            format!("meme:{}", label),
            count.to_string()
        ])?;
    }
    stmt.execute(rusqlite::params!["markdown", markdown])?;
    Ok(())
}

fn section_counts(conn: &Connection) -> Result<(usize, usize)> {
    let resp: usize = conn
        .prepare("SELECT COUNT(*) FROM job_sections WHERE responsibilities IS NOT NULL AND responsibilities <> ''")?
        .query_row([], |r| r.get(0))
        .unwrap_or(0);
    let req: usize = conn
        .prepare("SELECT COUNT(*) FROM job_sections WHERE requirements IS NOT NULL AND requirements <> ''")?
        .query_row([], |r| r.get(0))
        .unwrap_or(0);
    Ok((resp, req))
}

fn contains_any(text: &str, needles: &[&str], use_word_boundary: bool) -> bool {
    let lower = text.to_lowercase();
    if use_word_boundary {
        return needles.iter().any(|n| contains_word(&lower, n));
    }
    needles.iter().any(|n| lower.contains(n))
}

fn percent(part: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}
