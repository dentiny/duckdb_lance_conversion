#include "arrow_source_scan.hpp"

#include "lance_ffi.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/external_dependencies.hpp"
#include "duckdb/main/query_profiler.hpp"

#include <atomic>

namespace duckdb {
namespace {

// Owns the factory and reports its cumulative metrics to DuckDB as deltas.
class SourceStreamMetricsDependency final : public DependencyItem {
public:
	explicit SourceStreamMetricsDependency(SourceStreamFactory *factory_p) : factory(factory_p, lance_source_destroy) {
	}

	LanceReadMetrics Snapshot() const {
		LanceReadMetrics metrics;
		lance_source_get_metrics(factory.get(), &metrics);
		return metrics;
	}

	static uint64_t TakeDelta(std::atomic<uint64_t> &reported, uint64_t current) {
		auto previous = reported.load(std::memory_order_relaxed);
		while (current > previous && !reported.compare_exchange_weak(previous, current, std::memory_order_relaxed,
		                                                             std::memory_order_relaxed)) {
		}
		return current > previous ? current - previous : 0;
	}

	void ReportBytes(ClientContext &context) {
		auto delta = TakeDelta(reported_bytes, Snapshot().bytes_read);
		if (delta > 0) {
			QueryProfiler::Get(context).AddToCounter(MetricType::TOTAL_BYTES_READ, delta);
		}
	}

	std::atomic<uint64_t> reported_bytes {0};
	std::atomic<uint64_t> reported_rows {0};

private:
	std::unique_ptr<SourceStreamFactory, decltype(&lance_source_destroy)> factory;
};

SourceStreamMetricsDependency &GetDependency(const FunctionData *bind_data) {
	auto &arrow_data = bind_data->Cast<ArrowScanFunctionData>();
	if (!arrow_data.dependency) {
		throw InternalException("source scan has no metrics dependency");
	}
	return arrow_data.dependency->Cast<SourceStreamMetricsDependency>();
}

void SourceStreamScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	ArrowTableFunction::ArrowScanFunction(context, input, output);
	GetDependency(input.bind_data.get()).ReportBytes(context);
}

void SourceStreamGetMetrics(ClientContext &context, const FunctionData *bind_data, GlobalTableFunctionState &,
                            LocalTableFunctionState &, const profiler_settings_t &requested_metrics,
                            profiler_metrics_t &metrics) {
	auto &dependency = GetDependency(bind_data);
	dependency.ReportBytes(context);
	if (requested_metrics.find(MetricType::OPERATOR_ROWS_SCANNED) == requested_metrics.end()) {
		return;
	}
	auto rows = bind_data->Cast<ArrowScanFunctionData>().lines_read.load();
	metrics[MetricType::OPERATOR_ROWS_SCANNED] = Value::UBIGINT(dependency.TakeDelta(dependency.reported_rows, rows));
}

double SourceStreamProgress(ClientContext &, const FunctionData *bind_data, const GlobalTableFunctionState *) {
	auto snapshot = GetDependency(bind_data).Snapshot();
	if (snapshot.total_bytes == 0) {
		return -1.0;
	}
	return MinValue(100.0,
	                100.0 * static_cast<double>(snapshot.bytes_read) / static_cast<double>(snapshot.total_bytes));
}

// Same as DuckDB's protected ArrowTableFunction::ArrowGetPartitionData.
OperatorPartitionData SourceStreamGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("Arrow source scans do not support partition columns");
	}
	return OperatorPartitionData(input.local_state->Cast<ArrowScanLocalState>().batch_index);
}

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
	TableFunction function(name, {LogicalType::VARCHAR}, SourceStreamScan, bind,
	                       ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	function.get_partition_data = SourceStreamGetPartitionData;
	function.get_metrics = SourceStreamGetMetrics;
	function.table_scan_progress = SourceStreamProgress;
	function.projection_pushdown = true;
	return function;
}

} // namespace duckdb
