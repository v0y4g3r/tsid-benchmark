use std::process::Command;

fn main() {
    // Tell Cargo to rerun this if the schema changes
    println!("cargo:rerun-if-changed=schemas/label_row.fbs");

    // Generate Rust code from FlatBuffer schema
    let output = Command::new("flatc")
        .args([
            "--rust",
            "-o",
            "src/generated",
            "schemas/label_row.fbs",
        ])
        .output()
        .expect("Failed to run flatc. Make sure flatc is installed: https://github.com/google/flatbuffers/releases");

    if !output.status.success() {
        panic!(
            "flatc failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
