#include "arrow_source_scan.hpp"

#include "lance_ffi.hpp"
#include "source_metrics.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace {

class SourceStreamMetricsDependency final : public SourceMetricsDependency {
public:
	explicit SourceStreamMetricsDependency(SourceStreamFactory *factory_p) : factory(factory_p, lance_source_destroy) {
	}

	LanceReadMetrics Snapshot() const override {
		LanceReadMetrics metrics;
		lance_source_get_metrics(factory.get(), &metrics);
		return metrics;
	}

private:
	std::unique_ptr<SourceStreamFactory, decltype(&lance_source_destroy)> factory;
};

unique_ptr<ArrowArrayStreamWrapper> ProduceSourceStream(uintptr_t factory_ptr, ArrowStreamParameters &parameters) {
	auto result = make_uniq<ArrowArrayStreamWrapper>();
	vector<const char *> columns;
	columns.reserve(parameters.projected_columns.columns.size());
	for (const auto &column : parameters.projected_columns.columns) {
		columns.push_back(column.c_str());
	}
	ThrowIfLanceError(lance_source_get_stream(reinterpret_cast<SourceStreamFactory *>(factory_ptr), columns.data(),
	                                          columns.size(), &result->arrow_array_stream),
	                  "Source reader");
	return result;
}

} // namespace

OperatorPartitionData ArrowSourceGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("Arrow source scans do not support partition columns");
	}
	return OperatorPartitionData(input.local_state->Cast<ArrowScanLocalState>().batch_index);
}

unique_ptr<FunctionData> BindSourceStream(ClientContext &context, SourceStreamFactory *factory,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto dependency = make_shared_ptr<SourceStreamMetricsDependency>(factory);
	auto result = make_uniq<ArrowScanFunctionData>(ProduceSourceStream, reinterpret_cast<uintptr_t>(factory),
	                                               std::move(dependency));
	ThrowIfLanceError(lance_source_get_schema(factory, &result->schema_root.arrow_schema), "Source reader");
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = result->arrow_table.GetNames();
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	return std::move(result);
}

TableFunction SourceStreamTableFunction(const string &name, table_function_bind_t bind) {
	TableFunction function(name, {LogicalType::VARCHAR}, SourceMetricsArrowScan, bind,
	                       ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	function.get_partition_data = ArrowSourceGetPartitionData;
	function.get_metrics = SourceMetricsGetMetrics;
	function.table_scan_progress = SourceMetricsProgress;
	function.projection_pushdown = true;
	return function;
}

} // namespace duckdb
