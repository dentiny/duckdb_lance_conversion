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
duckdb_extension_load(json)
duckdb_extension_load(parquet)

if(BUILD_UNITTESTS)
  duckdb_extension_load(
    lance GIT_URL https://github.com/lance-format/lance-duckdb.git
    # Match the test reader selected by DuckDB v2.0.1's lance.cmake.
    GIT_TAG 86118537da79138e6ec5c7358f2384c7ae650ab0)
endif()
