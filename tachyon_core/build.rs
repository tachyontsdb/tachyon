use cbindgen::{Builder, Config, Error};
use std::env;
use std::path::Path;

fn generate_header(config_file: impl AsRef<Path>, header_file: impl AsRef<Path>) {
    let crate_directory = env::var("CARGO_MANIFEST_DIR").unwrap();
    Builder::new()
        .with_crate(crate_directory)
        .with_config(Config::from_file(config_file).unwrap())
        .generate()
        .map_or_else(
            |error| match error {
                Error::ParseSyntaxError { .. } => {}
                e => panic!("{:?}", e),
            },
            |bindings| {
                bindings.write_to_file(header_file);
            },
        )
}

pub fn main() {
    generate_header("cbindgen_c.toml", "../target/include/Tachyon.h");
    generate_header("cbindgen_cpp.toml", "../target/include/Tachyon.hpp");
}
