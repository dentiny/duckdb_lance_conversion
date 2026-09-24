#pragma once

#include "lance_conversion.h"
#include "duckdb/function/table/arrow.hpp"
#include "duckdb/main/external_dependencies.hpp"

#include <atomic>

namespace duckdb {

class SourceMetricsDependency : public DependencyItem {
public:
	~SourceMetricsDependency() override = default;
	virtual LanceReadMetrics Snapshot() const = 0;

private:
	friend void SourceMetricsArrowScan(ClientContext &, TableFunctionInput &, DataChunk &);
	friend void SourceMetricsGetMetrics(ClientContext &, const FunctionData *, GlobalTableFunctionState &,
	                                    LocalTableFunctionState &, const profiler_settings_t &, profiler_metrics_t &);
	friend double SourceMetricsProgress(ClientContext &, const FunctionData *, const GlobalTableFunctionState *);

	uint64_t TakeDelta(std::atomic<uint64_t> &reported, uint64_t current);
	void ReportBytes(ClientContext &context);

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
