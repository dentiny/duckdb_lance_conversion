#include "huggingface_scan.hpp"

#include "function_metadata.hpp"
#include "lance_ffi.hpp"
#include "lance_conversion.h"
#include "source_metrics.hpp"
#include "source_options.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/main/secret/secret.hpp"

namespace duckdb {
namespace {

class HuggingFaceMetricsDependency final : public SourceMetricsDependency {
public:
	explicit HuggingFaceMetricsDependency(HuggingFaceStreamFactory *factory_p)
	    : factory(factory_p, lance_huggingface_destroy) {
	}

	LanceReadMetrics Snapshot() const override {
		LanceReadMetrics metrics;
		lance_huggingface_get_metrics(factory.get(), &metrics);
		return metrics;
	}

private:
	std::unique_ptr<HuggingFaceStreamFactory, decltype(&lance_huggingface_destroy)> factory;
};

unique_ptr<ArrowArrayStreamWrapper> ProduceHuggingFaceStream(uintptr_t factory_ptr, ArrowStreamParameters &) {
	auto result = make_uniq<ArrowArrayStreamWrapper>();
	ThrowIfLanceError(lance_huggingface_get_stream(reinterpret_cast<HuggingFaceStreamFactory *>(factory_ptr),
	                                               &result->arrow_array_stream),
	                  "Hugging Face reader");
	return result;
}

unique_ptr<FunctionData> BindHuggingFace(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
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

	string token;
	KeyValueSecretReader secret_reader(*context.db, "huggingface", "hf://datasets/" + dataset);
	secret_reader.TryGetSecretKey("token", token);

	HuggingFaceStreamFactory *factory = nullptr;
	ThrowIfLanceError(lance_huggingface_open(dataset.c_str(), config.c_str(), split.c_str(),
	                                         token.empty() ? nullptr : token.c_str(), preserve_insertion_order,
	                                         read_options.max_read_parallelism, &factory),
	                  "Hugging Face reader");
	auto dependency = make_shared_ptr<HuggingFaceMetricsDependency>(factory);
	auto result = make_uniq<ArrowScanFunctionData>(ProduceHuggingFaceStream, reinterpret_cast<uintptr_t>(factory),
	                                               std::move(dependency));
	ThrowIfLanceError(lance_huggingface_get_schema(factory, &result->schema_root.arrow_schema), "Hugging Face reader");
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	result->projection_pushdown_enabled = false;
	if (return_types.empty()) {
		throw InvalidInputException("Hugging Face dataset must have at least one column");
	}
	return std::move(result);
}

} // namespace

void RegisterHuggingFaceScanFunction(ExtensionLoader &loader) {
	TableFunction function("read_huggingface", {LogicalType::VARCHAR}, SourceMetricsArrowScan, BindHuggingFace,
	                       ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	function.get_metrics = SourceMetricsGetMetrics;
	function.table_scan_progress = SourceMetricsProgress;
	function.named_parameters["config"] = LogicalType::VARCHAR;
	function.named_parameters["split"] = LogicalType::VARCHAR;
	function.named_parameters["preserve_insertion_order"] = LogicalType::BOOLEAN;
	SourceReadOptions::Register(function);
	RegisterTableFunctionWithMetadata(
	    loader, std::move(function),
	    /*parameter_names=*/
	    {"dataset", "config", "split", "preserve_insertion_order", "max_read_parallelism"},
	    /*description=*/"Reads a Hugging Face dataset's Parquet files.",
	    /*examples=*/ {"SELECT * FROM read_huggingface('lhoestq/demo1');"},
	    /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
