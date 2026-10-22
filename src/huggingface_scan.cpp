#include "huggingface_scan.hpp"

#include "arrow_source_scan.hpp"
#include "function_metadata.hpp"
#include "lance_ffi.hpp"
#include "lance_conversion.h"
#include "source_options.hpp"
#include "storage_options.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {
namespace {

unique_ptr<FunctionData> BindHuggingFace(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	RejectNullParameters("read_huggingface", "dataset", input);
	auto dataset = input.inputs[0].GetValue<string>();
	auto config = string("default");
	auto split = string("train");
	auto config_entry = input.named_parameters.find("config");
	if (config_entry != input.named_parameters.end()) {
		config = config_entry->second.GetValue<string>();
	}
	auto split_entry = input.named_parameters.find("split");
	if (split_entry != input.named_parameters.end()) {
		split = split_entry->second.GetValue<string>();
	}
	auto preserve_insertion_order = true;
	auto preserve_insertion_order_entry = input.named_parameters.find("preserve_insertion_order");
	if (preserve_insertion_order_entry != input.named_parameters.end()) {
		preserve_insertion_order = preserve_insertion_order_entry->second.GetValue<bool>();
	}
	auto read_options = SourceReadOptions::From(input);
	auto read_ahead_bytes = lance_source_defaults().read_ahead_bytes;
	auto read_ahead_bytes_entry = input.named_parameters.find("read_ahead_bytes");
	if (read_ahead_bytes_entry != input.named_parameters.end()) {
		read_ahead_bytes = read_ahead_bytes_entry->second.GetValue<uint64_t>();
	}

	string token;
	KeyValueSecretReader secret_reader(*context.db, "huggingface", "hf://datasets/" + dataset);
	secret_reader.TryGetSecretKey("token", token);

	auto opendal_config = ReadOpendalConfig(context);
	SourceStreamFactory *factory = nullptr;
	ThrowIfLanceError(lance_huggingface_open(dataset.c_str(), config.c_str(), split.c_str(),
	                                         token.empty() ? nullptr : token.c_str(), &opendal_config,
	                                         preserve_insertion_order, read_options.max_read_parallelism,
	                                         read_ahead_bytes, &factory),
	                  "Hugging Face reader");
	auto result = BindSourceStream(context, factory, return_types, names);
	if (return_types.empty()) {
		throw InvalidInputException("Hugging Face dataset must have at least one column");
	}
	return result;
}

} // namespace

void RegisterHuggingFaceScanFunction(ExtensionLoader &loader) {
	auto function = SourceStreamTableFunction("read_huggingface", BindHuggingFace);
	function.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
		options.Add("config", LogicalType::VARCHAR);
		options.Add("split", LogicalType::VARCHAR);
		options.Add("preserve_insertion_order", LogicalType::BOOLEAN);
		options.Add("read_ahead_bytes", LogicalType::UBIGINT);
	});
	SourceReadOptions::Register(function);
	RegisterTableFunctionWithMetadata(
	    loader, std::move(function),
	    /*parameter_names=*/
	    {"dataset", "config", "split", "preserve_insertion_order", "max_read_parallelism", "read_ahead_bytes"},
	    /*description=*/"Reads a Hugging Face dataset's Parquet files.",
	    /*examples=*/ {"SELECT * FROM read_huggingface('lhoestq/demo1');"},
	    /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
