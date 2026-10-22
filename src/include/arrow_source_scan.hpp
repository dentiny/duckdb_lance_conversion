#pragma once

#include "lance_conversion.h"
#include "duckdb/function/table/arrow.hpp"

namespace duckdb {

// Takes ownership of factory and binds an Arrow scan to its fixed schema.
unique_ptr<FunctionData> BindSourceStream(ClientContext &context, SourceStreamFactory *factory,
                                          vector<LogicalType> &return_types, vector<Identifier> &names);

// A projection-pushdown Arrow scan over BindSourceStream data, with metrics and progress.
TableFunction SourceStreamTableFunction(const Identifier &name, table_function_bind_t bind);

} // namespace duckdb
