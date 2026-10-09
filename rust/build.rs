use std::{env, fs, path::PathBuf};

fn main() {
    let header = "include/lance_storage_defaults.h";
    println!("cargo:rerun-if-changed={header}");

    let source = fs::read_to_string(header).expect("reading shared storage defaults");
    let mut generated =
        String::from("// Generated from include/lance_storage_defaults.h. Do not edit.\n");
    for line in source.lines() {
        let Some(definition) = line.trim().strip_prefix("inline constexpr ") else {
            continue;
        };
        let definition = definition.strip_suffix(';').expect("constant semicolon");
        let (declaration, value) = definition.split_once('=').expect("constant initializer");
        let mut tokens = declaration.split_whitespace();
        let cpp_type = tokens.next().expect("constant type");
        let name = tokens.next().expect("constant name");
        if !name.starts_with("LANCE_DEFAULT_STORAGE_") {
            continue;
        }
        assert!(tokens.next().is_none(), "unexpected tokens in {name}");
        let value = value.trim();
        let rust_type = match cpp_type {
            "uint64_t" => {
                value.parse::<u64>().expect("integer storage default");
                "u64"
            }
            "double" => {
                value
                    .parse::<f64>()
                    .expect("floating-point storage default");
                "f64"
            }
            _ => panic!("unsupported storage default type: {cpp_type}"),
        };
        generated.push_str(&format!("pub const {name}: {rust_type} = {value};\n"));
    }

    let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo OUT_DIR"));
    fs::write(output.join("storage_defaults.rs"), generated)
        .expect("writing Rust storage defaults");
}
