#pragma once

#include <cstdint>

namespace duckdb {

struct TableFunctionBindInput;
class TableFunction;

struct SourceReadOptions {
	static constexpr uint64_t DEFAULT_MAX_READ_PARALLELISM = 8;

	uint64_t max_read_parallelism = DEFAULT_MAX_READ_PARALLELISM;

	static SourceReadOptions From(const TableFunctionBindInput &input);
	static void Register(TableFunction &function);
};

} // namespace duckdb
