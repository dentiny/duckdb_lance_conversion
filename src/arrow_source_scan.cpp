#include "arrow_source_scan.hpp"

#include "lance_ffi.hpp"
#include "source_metadata.hpp"

#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/main/query_profiler.hpp"

#include <atomic>

namespace duckdb {
namespace {

// Owns the factory and reports its cumulative metrics to DuckDB as deltas.
class LanceArrowScanFactory final : public ArrowScanFactory {
public:
	explicit LanceArrowScanFactory(SourceStreamFactory *factory_p) : factory(factory_p, lance_source_destroy) {
	}

	void GetSchema(ArrowSchema &schema) override {
		ThrowIfLanceError(lance_source_get_schema(factory.get(), &schema), "Source reader");
	}

	unique_ptr<ArrowArrayStreamWrapper> ProduceStream(ArrowStreamParameters &parameters) override {
		auto result = make_uniq<ArrowArrayStreamWrapper>();
		vector<const char *> columns;
		columns.reserve(parameters.projected_columns.columns.size());
		for (const auto &column : parameters.projected_columns.columns) {
			columns.push_back(column.c_str());
		}
		ThrowIfLanceError(
		    lance_source_get_stream(factory.get(), columns.data(), columns.size(), &result->arrow_array_stream),
		    "Source reader");
		return result;
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
			QueryProfiler::Get(context).TrackBytesRead(delta);
		}
	}

	std::atomic<uint64_t> reported_bytes {0};
	std::atomic<uint64_t> reported_rows {0};

private:
	std::unique_ptr<SourceStreamFactory, decltype(&lance_source_destroy)> factory;
};

LanceArrowScanFactory &GetFactory(const FunctionData *bind_data) {
	auto &arrow_data = bind_data->Cast<ArrowScanFunctionData>();
	if (!arrow_data.factory) {
		throw InternalException("source scan has no Arrow factory");
	}
	return arrow_data.factory->Cast<LanceArrowScanFactory>();
}

void SourceStreamScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	ArrowTableFunction::ArrowScanFunction(context, input, output);
	GetFactory(input.bind_data.get()).ReportBytes(context);
}

void SourceStreamGetMetrics(TableFunctionGetMetricsInput &input) {
	auto &factory = GetFactory(input.bind_data.get());
	factory.ReportBytes(input.context);
	auto rows = input.bind_data->Cast<ArrowScanFunctionData>().lines_read.load();
	input.operator_metrics.rows_scanned = factory.TakeDelta(factory.reported_rows, rows);
}

double SourceStreamProgress(ClientContext &, const FunctionData *bind_data, const GlobalTableFunctionState *) {
	auto snapshot = GetFactory(bind_data).Snapshot();
	if (snapshot.total_bytes == 0) {
		return -1.0;
	}
	return MinValue(100.0,
	                100.0 * static_cast<double>(snapshot.bytes_read) / static_cast<double>(snapshot.total_bytes));
}

struct SourceArrowTableFunction : ArrowTableFunction {
	using ArrowTableFunction::ArrowGetPartitionData;
};

} // namespace

unique_ptr<FunctionData> BindSourceStream(ClientContext &context, SourceStreamFactory *factory,
                                          vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto arrow_factory = make_shared_ptr<LanceArrowScanFactory>(factory);
	auto result = make_uniq<ArrowScanFunctionData>(std::move(arrow_factory));
	result->factory->GetSchema(result->schema_root.arrow_schema);
	ArrowTableFunction::PopulateArrowTableSchema(context, result->arrow_table, result->schema_root.arrow_schema);
	names = StringsToIdentifiers(result->arrow_table.GetNames());
	ImportSourceMetadata(result->arrow_table.GetTypes(), result->schema_root.arrow_schema);
	return_types = result->arrow_table.GetTypes();
	result->all_types = return_types;
	return result;
}

TableFunction SourceStreamTableFunction(const Identifier &name, table_function_bind_t bind) {
	TableFunction function(name, {LogicalType::VARCHAR}, SourceStreamScan, bind,
	                       ArrowTableFunction::ArrowScanInitGlobal, ArrowTableFunction::ArrowScanInitLocal);
	function.get_partition_data = SourceArrowTableFunction::ArrowGetPartitionData;
	function.get_metrics = SourceStreamGetMetrics;
	function.table_scan_progress = SourceStreamProgress;
	function.projection_pushdown = true;
	return function;
}

} // namespace duckdb
