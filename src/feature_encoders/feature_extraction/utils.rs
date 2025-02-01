

pub fn default_tokenizer(s: &str) -> Vec<String> {
    s
        .split_whitespace()
        .filter(|word| !word.is_empty())
        .map(|word| word.to_string())
        .collect()
}