#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class ExtensionLoader;

void RegisterTableFunctionWithMetadata(ExtensionLoader &loader, TableFunction function, vector<string> parameter_names,
                                       string description, vector<string> examples, vector<string> categories);

} // namespace duckdb
