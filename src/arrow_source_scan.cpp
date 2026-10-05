#include "arrow_source_scan.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

OperatorPartitionData ArrowSourceGetPartitionData(ClientContext &context, TableFunctionGetPartitionInput &input) {
	if (input.partition_info.RequiresPartitionColumns()) {
		throw InternalException("Arrow source scans do not support partition columns");
	}
	return OperatorPartitionData(input.local_state->Cast<ArrowScanLocalState>().batch_index);
}

} // namespace duckdb
