#pragma once

#include "duckdb/common/arrow/arrow.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

// Preserve source/field semantics in DuckDB's serializable type properties.
void ImportSourceMetadata(vector<LogicalType> &types, const ArrowSchema &schema);
// Attach metadata to a schema created by ArrowConverter::ToArrowSchema, using its owner.
void ExportSourceMetadata(const vector<LogicalType> &types, ArrowSchema &schema);

} // namespace duckdb
