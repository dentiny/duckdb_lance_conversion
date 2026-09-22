PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
EXT_NAME=lance_conversion
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: rust-test
rust-test:
	cargo test --locked --manifest-path rust/Cargo.toml
