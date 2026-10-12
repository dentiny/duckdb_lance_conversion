#include "atif_scan.hpp"

#include "arrow_source_scan.hpp"
#include "function_metadata.hpp"
#include "lance_conversion.h"
#include "lance_ffi.hpp"
#include "source_metrics.hpp"
#include "storage_options.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/external_dependencies.hpp"

namespace duckdb {
namespace {

class AtifMetricsDependency final : public SourceMetricsDependency {
public:
	explicit AtifMetricsDependency(AtifStreamFactory *factory_p) : factory(factory_p, lance_atif_destroy) {
	}

	LanceReadMetrics Snapshot() const override {
		LanceReadMetrics metrics;
		lance_atif_get_metrics(factory.get(), &metrics);
		return metrics;
	}

private:
	std::unique_ptr<AtifStreamFactory, decltype(&lance_atif_destroy)> factory;
};

unique_ptr<ArrowArrayStreamWrapper> ProduceAtifStream(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
	auto result = make_uniq<ArrowArrayStreamWrapper>();
	vector<const char *> columns;
	columns.reserve(parameters.projected_columns.columns.size());
	for (const auto &column : parameters.projected_columns.columns) {
		columns.push_back(column.c_str());
	}
	ThrowIfLanceError(lance_atif_get_stream(reinterpret_cast<AtifStreamFactory *>(factory_ptr), columns.data(),
	                                        columns.size(), &result->arrow_array_stream),
	                  "ATIF reader");
	return result;
}

unique_ptr<FunctionData> BindAtif(ClientContext &context, TableFunctionBindInput &input,
                                  vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs[0].IsNull()) {
		throw BinderException("read_atif path must not be NULL");
	}
	auto path = input.inputs[0].GetValue<string>();
	string format;
	auto format_entry = input.named_parameters.find("format");
	if (format_entry != input.named_parameters.end()) {
		format = format_entry->second.GetValue<string>();
	}
	LanceS3Config s3;
	const LanceS3Config *s3_ptr = nullptr;
	LanceS3Options options;
	if (FileSystem::IsRemoteFile(path)) {
		if (!StringUtil::CIStartsWith(path, "s3://")) {
			throw BinderException("read_atif supports only local and s3:// paths");
		}
		options = ReadS3Options(context, path);
		s3 = options.ToConfig();
		s3_ptr = &s3;
	}

	auto opendal_config = ReadOpendalConfig(context);
	AtifStreamFactory *factory = nullptr;
	ThrowIfLanceError(
	    lance_atif_open(path.c_str(), format.empty() ? nullptr : format.c_str(), s3_ptr, &opendal_config, &factory),
	    "ATIF reader");
	auto dependency = make_shared_ptr<AtifMetricsDependency>(factory);
	auto result = make_uniq<ArrowScanFunctionData>(ProduceAtifStream, reinterpret_cast<uintptr_t>(factory),
	                                               std::move(dependency));
	ThrowIfLanceError(lance_atif_get_schema(factory, &result->schema_root.arrow_schema), "ATIF reader");
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	return std::move(result);
}

} // namespace

void RegisterAtifScanFunction(ExtensionLoader &loader) {
	TableFunction function("read_atif", {LogicalType::VARCHAR}, SourceMetricsArrowScan, BindAtif,
	                       ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	function.get_partition_data = ArrowSourceGetPartitionData;
	function.get_metrics = SourceMetricsGetMetrics;
	function.table_scan_progress = SourceMetricsProgress;
	function.projection_pushdown = true;
	function.named_parameters["format"] = LogicalType::VARCHAR;
	RegisterTableFunctionWithMetadata(
	    loader, std::move(function),
	    /*parameter_names=*/ {"path", "format"},
	    /*description=*/"Reads ATIF agent trajectories from a local or S3 JSON or JSONL file.",
	    /*examples=*/ {"SELECT * FROM read_atif('trajectories.jsonl');"},
	    /*categories=*/ {"lance_conversion", "reader"});
}

} // namespace duckdb
