use crate::db;
use anyhow::Result;
use rusqlite::Connection;

pub fn run(conn: &Connection) -> Result<usize> {
    let rows = db::fetch_shortened(conn)?;
    let mut stmt_body = conn.prepare(
        r#"
        INSERT OR REPLACE INTO job_body (
            job_id, url, role_description, body_ok, updated_at
        ) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
        "#,
    )?;

    let mut stmt_sections = conn.prepare(
        r#"
        INSERT OR REPLACE INTO job_sections (
            job_id, url, responsibilities, requirements, nice_to_have, benefits, summary, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        "#,
    )?;

    let mut written = 0;
    for row in rows {
        let sections = extract_sections(&row.text_shortened);
        let role_description = sections.summary.clone();
        let body_ok = !sections.responsibilities.is_empty() || !sections.requirements.is_empty();

        stmt_body.execute(rusqlite::params![
            row.job_id,
            row.url,
            role_description,
            bool_to_int(body_ok),
        ])?;

        stmt_sections.execute(rusqlite::params![
            row.job_id,
            row.url,
            join_lines(&sections.responsibilities),
            join_lines(&sections.requirements),
            join_lines(&sections.nice_to_have),
            join_lines(&sections.benefits),
            sections.summary,
        ])?;

        written += 1;
    }

    Ok(written)
}

fn join_lines(lines: &[String]) -> Option<String> {
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

fn bool_to_int(b: bool) -> i64 {
    if b {
        1
    } else {
        0
    }
}

#[derive(Default, Debug)]
struct BodySections {
    responsibilities: Vec<String>,
    requirements: Vec<String>,
    nice_to_have: Vec<String>,
    benefits: Vec<String>,
    summary: Option<String>,
}

fn extract_sections(text: &str) -> BodySections {
    let lines: Vec<String> = text
        .lines()
        .map(|l| l.trim().trim_start_matches("- ").trim_start_matches("• ").to_string())
        .filter(|l| !l.is_empty())
        .collect();

    let mut sections = BodySections::default();
    let mut current: Option<&str> = None;

    for line in lines {
        let lower = line.to_lowercase();
        if is_responsibilities_header(&lower) {
            current = Some("responsibilities");
            continue;
        }
        if is_requirements_header(&lower) {
            current = Some("requirements");
            continue;
        }
        if is_nice_header(&lower) {
            current = Some("nice");
            continue;
        }
        if is_benefits_header(&lower) {
            current = Some("benefits");
            continue;
        }

        match current {
            Some("responsibilities") => sections.responsibilities.push(line.clone()),
            Some("requirements") => sections.requirements.push(line.clone()),
            Some("nice") => sections.nice_to_have.push(line.clone()),
            Some("benefits") => sections.benefits.push(line.clone()),
            None => {
                // Prime responsibilities if we haven't seen a header yet.
                if sections.responsibilities.len() < 5 {
                    sections.responsibilities.push(line.clone());
                }
            }
            _ => {}
        }
    }

    // Summary: take first 3 bullets from responsibilities or requirements.
    let resp_summary = sections
        .responsibilities
        .iter()
        .take(3)
        .cloned()
        .collect::<Vec<_>>()
        .join("\n");
    if !resp_summary.is_empty() {
        sections.summary = Some(resp_summary);
    }
    if sections.summary.is_none() && !sections.requirements.is_empty() {
        let req_summary = sections
            .requirements
            .iter()
            .take(2)
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        if !req_summary.is_empty() {
            sections.summary = Some(req_summary);
        }
    }

    sections
}

fn is_responsibilities_header(line_lower: &str) -> bool {
    line_lower.starts_with("about the role")
        || line_lower.starts_with("about the job")
        || line_lower.starts_with("what you will do")
        || line_lower.starts_with("what you'll do")
        || line_lower.starts_with("responsibilities")
        || line_lower.starts_with("role description")
}

fn is_requirements_header(line_lower: &str) -> bool {
    line_lower.starts_with("requirements")
        || line_lower.starts_with("qualifications")
        || line_lower.starts_with("what we're looking for")
        || line_lower.starts_with("who you are")
        || line_lower.starts_with("must have")
        || line_lower.starts_with("must-haves")
}

fn is_nice_header(line_lower: &str) -> bool {
    line_lower.starts_with("nice to have")
        || line_lower.starts_with("nice-to-haves")
        || line_lower.starts_with("preferred")
        || line_lower.starts_with("bonus")
}

fn is_benefits_header(line_lower: &str) -> bool {
    line_lower.starts_with("benefits") || line_lower.starts_with("perks")
}
