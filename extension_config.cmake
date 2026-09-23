# Cargo package version is the source of truth for the extension metadata.
file(
  STRINGS "${CMAKE_CURRENT_LIST_DIR}/rust/Cargo.toml" LANCE_PACKAGE_VERSION_LINE
  REGEX "^version = \"[^\"]+\"$"
  LIMIT_COUNT 1)
string(REGEX REPLACE "^version = \"([^\"]+)\"$" "\\1" LANCE_CONVERSION_VERSION
                     "${LANCE_PACKAGE_VERSION_LINE}")
if("${LANCE_CONVERSION_VERSION}" STREQUAL "")
  message(
    FATAL_ERROR "Cannot read lance-conversion package version from Cargo.toml")
endif()

duckdb_extension_load(lance_conversion SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
                      EXTENSION_VERSION ${LANCE_CONVERSION_VERSION})
duckdb_extension_load(parquet)

if(BUILD_UNITTESTS)
  duckdb_extension_load(
    lance GIT_URL https://github.com/lance-format/lance-duckdb.git
    # Includes the Rust 1.97 / ethnum compatibility fix and targets DuckDB 1.5.
    GIT_TAG 2f167ea1aa8b1201c89d53740b84deb00aff680e DONT_LINK)
endif()
