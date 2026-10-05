#pragma once

#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

// Same as DuckDB's protected ArrowTableFunction::ArrowGetPartitionData.
OperatorPartitionData ArrowSourceGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input);

} // namespace duckdb
