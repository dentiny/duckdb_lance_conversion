PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
EXT_NAME=lance_conversion
EXT_CONFIG=${PROJ_DIR}extension_config.cmake
include extension-ci-tools/makefiles/duckdb_extension.Makefile

CMAKE_FILES := CMakeLists.txt extension_config.cmake

format-all: format
	cmake-format -i $(CMAKE_FILES)
	cargo fmt --manifest-path rust/Cargo.toml

.PHONY: format-all rust-test
rust-test:
	cargo test --locked --manifest-path rust/Cargo.toml
