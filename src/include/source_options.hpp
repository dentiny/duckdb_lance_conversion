#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct SourceReadOptions {
	static constexpr uint64_t DEFAULT_MAX_READ_PARALLELISM = 8;

	uint64_t max_read_parallelism = DEFAULT_MAX_READ_PARALLELISM;

	static SourceReadOptions From(const TableFunctionBindInput &input) {
		SourceReadOptions options;
		auto entry = input.named_parameters.find("max_read_parallelism");
		if (entry != input.named_parameters.end()) {
			options.max_read_parallelism = entry->second.GetValue<uint64_t>();
		}
		if (options.max_read_parallelism == 0) {
			throw InvalidInputException("max_read_parallelism must be positive");
		}
		return options;
	}

	static void Register(TableFunction &function) {
		function.named_parameters["max_read_parallelism"] = LogicalType::UBIGINT;
	}
};

} // namespace duckdb
