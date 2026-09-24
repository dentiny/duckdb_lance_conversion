#include "source_metrics.hpp"

#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

SourceMetricsDependency::SourceMetricsDependency(void *factory_p, destroy_t destroy_p, get_metrics_t get_metrics_p)
    : factory(factory_p), destroy(std::move(destroy_p)), get_metrics(std::move(get_metrics_p)) {
}

SourceMetricsDependency::~SourceMetricsDependency() {
	destroy(factory);
}

LanceReadMetrics SourceMetricsDependency::Snapshot() const {
	LanceReadMetrics result;
	get_metrics(factory, &result);
	return result;
}

uint64_t SourceMetricsDependency::TakeDelta(std::atomic<uint64_t> &reported, uint64_t current) {
	auto previous = reported.load(std::memory_order_relaxed);
	while (current > previous &&
	       !reported.compare_exchange_weak(previous, current, std::memory_order_relaxed, std::memory_order_relaxed)) {
	}
	return current > previous ? current - previous : 0;
}

void SourceMetricsDependency::ReportBytes(ClientContext &context) {
	auto snapshot = Snapshot();
	auto delta = TakeDelta(reported_bytes, snapshot.bytes_read);
	if (delta > 0) {
		QueryProfiler::Get(context).AddToCounter(MetricType::TOTAL_BYTES_READ, delta);
	}
}

SourceMetricsDependency &GetSourceMetricsDependency(const FunctionData *bind_data) {
	auto &arrow_data = bind_data->Cast<ArrowScanFunctionData>();
	if (!arrow_data.dependency) {
		throw InternalException("source scan has no metrics dependency");
	}
	return arrow_data.dependency->Cast<SourceMetricsDependency>();
}

void SourceMetricsArrowScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	ArrowTableFunction::ArrowScanFunction(context, input, output);
	GetSourceMetricsDependency(input.bind_data.get()).ReportBytes(context);
}

void SourceMetricsGetMetrics(ClientContext &context, const FunctionData *bind_data, GlobalTableFunctionState &,
                             LocalTableFunctionState &, const profiler_settings_t &requested_metrics,
                             profiler_metrics_t &metrics) {
	auto &dependency = GetSourceMetricsDependency(bind_data);
	dependency.ReportBytes(context);
	if (requested_metrics.find(MetricType::OPERATOR_ROWS_SCANNED) == requested_metrics.end()) {
		return;
	}
	auto &arrow_data = bind_data->Cast<ArrowScanFunctionData>();
	auto rows = arrow_data.lines_read.load();
	metrics[MetricType::OPERATOR_ROWS_SCANNED] = Value::UBIGINT(dependency.TakeDelta(dependency.reported_rows, rows));
}

double SourceMetricsProgress(ClientContext &, const FunctionData *bind_data, const GlobalTableFunctionState *) {
	auto snapshot = GetSourceMetricsDependency(bind_data).Snapshot();
	if (snapshot.total_bytes == 0) {
		return -1.0;
	}
	return MinValue(100.0,
	                100.0 * static_cast<double>(snapshot.bytes_read) / static_cast<double>(snapshot.total_bytes));
}

} // namespace duckdb
