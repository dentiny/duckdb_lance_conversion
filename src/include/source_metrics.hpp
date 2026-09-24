#pragma once

#include "lance_conversion.h"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/external_dependencies.hpp"

#include <atomic>
#include <functional>

namespace duckdb {

class SourceMetricsDependency : public DependencyItem {
public:
	using destroy_t = std::function<void(void *)>;
	using get_metrics_t = std::function<void(const void *, LanceReadMetrics *)>;

	SourceMetricsDependency(void *factory, destroy_t destroy, get_metrics_t get_metrics);
	~SourceMetricsDependency() override;

	LanceReadMetrics Snapshot() const;

private:
	friend void SourceMetricsArrowScan(ClientContext &, TableFunctionInput &, DataChunk &);
	friend void SourceMetricsGetMetrics(ClientContext &, const FunctionData *, GlobalTableFunctionState &,
	                                    LocalTableFunctionState &, const profiler_settings_t &, profiler_metrics_t &);
	friend double SourceMetricsProgress(ClientContext &, const FunctionData *, const GlobalTableFunctionState *);

	uint64_t TakeDelta(std::atomic<uint64_t> &reported, uint64_t current);
	void ReportBytes(ClientContext &context);

	void *factory;
	destroy_t destroy;
	get_metrics_t get_metrics;
	std::atomic<uint64_t> reported_bytes {0};
	std::atomic<uint64_t> reported_rows {0};
};

SourceMetricsDependency &GetSourceMetricsDependency(const FunctionData *bind_data);
void SourceMetricsArrowScan(ClientContext &context, TableFunctionInput &input, DataChunk &output);
void SourceMetricsGetMetrics(ClientContext &context, const FunctionData *bind_data, GlobalTableFunctionState &,
                             LocalTableFunctionState &, const profiler_settings_t &requested_metrics,
                             profiler_metrics_t &metrics);
double SourceMetricsProgress(ClientContext &, const FunctionData *bind_data, const GlobalTableFunctionState *);

} // namespace duckdb
