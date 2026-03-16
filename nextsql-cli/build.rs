use std::fs;
use std::path::Path;

fn main() {
    let source = Path::new("../.claude/skills/learn-next-sql/SKILL.md");
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let dest = Path::new(&out_dir).join("skill.md");

    println!("cargo:rerun-if-changed={}", source.display());

    let content = fs::read_to_string(source).expect("Failed to read SKILL.md");

    // Strip the References section (and everything after it)
    let output = if let Some(pos) = content.find("\n## References") {
        &content[..pos]
    } else {
        &content
    };

    fs::write(dest, output.trim_end()).expect("Failed to write skill.md");
}
