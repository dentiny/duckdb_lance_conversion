use std::{env, fs, path::PathBuf};

fn main() {
    let header = "include/lance_storage_defaults.h";
    println!("cargo:rerun-if-changed={header}");

    let source = fs::read_to_string(header).expect("reading shared storage defaults");
    let mut generated =
        String::from("// Generated from include/lance_storage_defaults.h. Do not edit.\n");
    for line in source.lines() {
        let Some(definition) = line.trim().strip_prefix("#define ") else {
            continue;
        };
        let mut tokens = definition.split_whitespace();
        let name = tokens.next().expect("constant name");
        if !name.starts_with("LANCE_DEFAULT_STORAGE_") {
            continue;
        }
        let value = tokens.next().expect("constant value");
        assert!(tokens.next().is_none(), "{name} must be a numeric literal");
        let rust_type = if name.ends_with("_FACTOR") {
            value
                .parse::<f64>()
                .expect("floating-point storage default");
            "f64"
        } else {
            value.parse::<u64>().expect("integer storage default");
            "u64"
        };
        generated.push_str(&format!("pub const {name}: {rust_type} = {value};\n"));
    }

    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo OUT_DIR"));
    fs::write(output.join("storage_defaults.rs"), generated)
        .expect("writing Rust storage defaults");
}
