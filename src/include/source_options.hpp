#pragma once

#include "duckdb/common/string.hpp"

#include <cstdint>

namespace duckdb {

struct TableFunctionBindInput;
class TableFunction;

struct SourceReadOptions {
	uint64_t max_read_parallelism;

	static SourceReadOptions From(const TableFunctionBindInput &input);
	static void Register(TableFunction &function);
};

void RejectNullParameters(const string &function_name, const string &argument_name,
                          const TableFunctionBindInput &input);

} // namespace duckdb
