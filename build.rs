use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

fn main() {
    let manifest_dir =
        env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR environment variable is set");
    let env_path = Path::new(&manifest_dir).join(".env");

    println!("cargo:rerun-if-changed={}", env_path.display());

    let mut values = if env_path.exists() {
        match read_env_file(&env_path) {
            Ok(vars) => vars,
            Err(err) => {
                println!(
                    "cargo:warning=Failed to parse {}: {err}. Falling back to defaults.",
                    env_path.display()
                );
                HashMap::new()
            }
        }
    } else {
        println!(
            "cargo:warning=.env file not found at {}. Using built-in defaults.",
            env_path.display()
        );
        HashMap::new()
    };

    emit_default("CLICKHOUSE_ADDRESS", &mut values, "http://localhost");
    emit_default("CLICKHOUSE_PORT", &mut values, "8123");
    emit_default("CLICKHOUSE_USER", &mut values, "default");
    emit_default("CLICKHOUSE_PASSWORD", &mut values, "");
    emit_default("CLICKHOUSE_DATABASE", &mut values, "eth_indexer");
    emit_default("ETH_NODE_URL", &mut values, "http://localhost:8545");
}

fn read_env_file(path: &Path) -> Result<HashMap<String, String>, String> {
    let content = fs::read_to_string(path)
        .map_err(|err| format!("unable to read {}: {err}", path.display()))?;

    let mut map = HashMap::new();
    for (line_no, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("missing '=' on line {}", line_no + 1))?;
        let key = key.trim();
        if key.is_empty() {
            return Err(format!("empty key on line {}", line_no + 1));
        }

        let value = value.trim();
        let unquoted = strip_quotes(value);
        map.insert(key.to_string(), unquoted);
    }

    Ok(map)
}

fn emit_default(key: &str, values: &mut HashMap<String, String>, fallback: &str) {
    let value = values.remove(key).unwrap_or_else(|| fallback.to_string());
    println!("cargo:rustc-env={key}={value}");
}

fn strip_quotes(value: &str) -> String {
    if value.len() >= 2 {
        let first = value.chars().next().unwrap();
        let last = value.chars().last().unwrap();
        if (first == '"' && last == '"') || (first == '\'' && last == '\'') {
            return value[1..value.len() - 1].to_string();
        }
    }
    value.to_string()
}
