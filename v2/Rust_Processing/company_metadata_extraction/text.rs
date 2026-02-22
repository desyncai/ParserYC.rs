use std::collections::HashSet;

#[derive(Debug, Clone)]
pub struct WorkingText {
    lines: Vec<String>,
}

impl WorkingText {
    pub fn from_raw(raw: &str) -> Self {
        let lines = raw
            .replace("\r\n", "\n")
            .split('\n')
            .map(|l| l.trim().to_string())
            .filter(|l| !l.is_empty())
            .collect();
        WorkingText { lines }
    }

    pub fn char_len(&self) -> usize {
        if self.lines.is_empty() {
            0
        } else {
            // add 1 per newline to keep length realistic
            self.lines.iter().map(|l| l.len() + 1).sum::<usize>().saturating_sub(1)
        }
    }

    #[allow(dead_code)]
    pub fn take_first(&mut self) -> Option<String> {
        if self.lines.is_empty() {
            None
        } else {
            Some(self.lines.remove(0))
        }
    }

    pub fn take_first_matching<F>(&mut self, f: F) -> Option<String>
    where
        F: Fn(&str) -> bool,
    {
        let idx = self.lines.iter().position(|l| f(l))?;
        Some(self.lines.remove(idx))
    }

    pub fn take_prefix_until_blank(&mut self) -> Vec<String> {
        let mut taken = Vec::new();
        while let Some(line) = self.lines.first() {
            if line.trim().is_empty() {
                self.lines.remove(0);
                break;
            }
            taken.push(line.clone());
            self.lines.remove(0);
            if taken.len() >= 6 {
                break;
            }
        }
        taken
    }

    pub fn remove_where<F>(&mut self, f: F) -> Vec<String>
    where
        F: Fn(&str) -> bool,
    {
        let mut removed = Vec::new();
        self.lines.retain(|line| {
            if f(line) {
                removed.push(line.clone());
                false
            } else {
                true
            }
        });
        removed
    }

    #[allow(dead_code)]
    pub fn retain_indices(&mut self, keep: &HashSet<usize>) {
        self.lines = self
            .lines
            .iter()
            .enumerate()
            .filter(|(idx, _)| keep.contains(idx))
            .map(|(_, val)| val.clone())
            .collect();
    }

    pub fn take_first_n(&mut self, n: usize) -> Vec<String> {
        let mut taken = Vec::new();
        for _ in 0..n.min(self.lines.len()) {
            if let Some(line) = self.lines.first().cloned() {
                taken.push(line);
                self.lines.remove(0);
            }
        }
        taken
    }

    pub fn sample(&self, max_chars: usize) -> String {
        let mut out = String::new();
        for line in &self.lines {
            if out.len() + line.len() + 1 > max_chars {
                break;
            }
            if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(line);
        }
        out
    }

    pub fn lines(&self) -> &[String] {
        &self.lines
    }
}

#[derive(Debug, Clone)]
pub struct WorkItem {
    pub url: String,
    pub slug: Option<String>,
    pub name: Option<String>,
    pub text: WorkingText,
    pub external_links: Vec<String>,
}

pub fn total_chars(items: &[WorkItem]) -> usize {
    items.iter().map(|i| i.text.char_len()).sum()
}
