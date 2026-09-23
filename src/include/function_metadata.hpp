#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

inline void RegisterTableFunctionWithMetadata(ExtensionLoader &loader, TableFunction function,
                                              vector<string> parameter_names, string description,
                                              vector<string> examples, vector<string> categories) {
	FunctionDescription metadata;
	metadata.parameter_names = std::move(parameter_names);
	metadata.description = std::move(description);
	metadata.examples = std::move(examples);
	metadata.categories = std::move(categories);

	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	info.descriptions.push_back(std::move(metadata));
	loader.RegisterFunction(std::move(info));
}

} // namespace duckdb
