cmake_minimum_required(VERSION 3.15)
project(lance_conversion)
set(TARGET_NAME lance_conversion)
set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
include_directories(src/include rust/include)

find_program(CARGO_EXECUTABLE cargo REQUIRED)
find_program(PROTOC_EXECUTABLE protoc REQUIRED)
set(LANCE_CARGO_PROFILE "release" CACHE STRING "Cargo profile: dev or release")
if(LANCE_CARGO_PROFILE STREQUAL "dev")
  set(CARGO_PROFILE_DIR debug)
else()
  set(CARGO_PROFILE_DIR ${LANCE_CARGO_PROFILE})
endif()
set(RUST_TARGET_DIR "${CMAKE_BINARY_DIR}/rust-lance-conversion")
set(RUST_LIB_DIR "${RUST_TARGET_DIR}")
if(DEFINED Rust_CARGO_TARGET AND NOT "${Rust_CARGO_TARGET}" STREQUAL "")
  set(RUST_TARGET_FLAG "--target=${Rust_CARGO_TARGET}")
  set(RUST_LIB_DIR "${RUST_LIB_DIR}/${Rust_CARGO_TARGET}")
endif()
set(RUST_LIB "${RUST_LIB_DIR}/${CARGO_PROFILE_DIR}/${CMAKE_STATIC_LIBRARY_PREFIX}lance_conversion${CMAKE_STATIC_LIBRARY_SUFFIX}")
file(GLOB_RECURSE RUST_SOURCES CONFIGURE_DEPENDS rust/src/*.rs rust/include/*.h rust/Cargo.toml rust/Cargo.lock)
add_custom_command(
  OUTPUT "${RUST_LIB}"
  COMMAND ${CMAKE_COMMAND} -E env --unset=CC --unset=CXX --unset=LD
    "PROTOC=${PROTOC_EXECUTABLE}" ${CARGO_EXECUTABLE} build --locked --lib
    --manifest-path "${CMAKE_CURRENT_SOURCE_DIR}/rust/Cargo.toml"
    --target-dir "${RUST_TARGET_DIR}" --profile ${LANCE_CARGO_PROFILE} ${RUST_TARGET_FLAG}
  DEPENDS ${RUST_SOURCES}
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/rust"
  COMMENT "Building Rust Lance conversion core"
  VERBATIM)
add_custom_target(lance_conversion_rust DEPENDS "${RUST_LIB}")

if(APPLE)
  set(PLATFORM_LIBS m c System resolv "-framework CoreFoundation" "-framework Security" "-framework SystemConfiguration")
elseif(UNIX)
  set(PLATFORM_LIBS m c dl pthread resolv)
elseif(WIN32)
  set(PLATFORM_LIBS ntdll crypt32 ncrypt secur32 ws2_32 userenv bcrypt advapi32)
endif()
set(EXTENSION_SOURCES src/lance_conversion_extension.cpp)
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
foreach(target ${EXTENSION_NAME} ${LOADABLE_EXTENSION_NAME})
  target_link_libraries(${target} "${RUST_LIB}" ${PLATFORM_LIBS})
  add_dependencies(${target} lance_conversion_rust)
endforeach()
install(TARGETS ${EXTENSION_NAME} EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}" ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
