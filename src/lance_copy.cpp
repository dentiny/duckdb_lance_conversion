#include "lance_copy.hpp"
#include "source_metadata.hpp"
#include "lance_ffi.hpp"
#include "storage_options.hpp"
#include "lance_conversion.h"
#include "duckdb/common/arrow/arrow_converter.hpp"
#include "duckdb/common/arrow/arrow_wrapper.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/parsed_data/sample_options.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/copy_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"

#include <algorithm>
#include <cmath>
#include <tuple>
#include <string_view>

namespace duckdb {
namespace {

// Lance encoding settings for one column; an empty compression and unset level or encoding keep Lance's defaults.
struct ColumnEncodingOptions {
	Identifier column;
	string compression;
	bool has_compression_level = false;
	int32_t compression_level = 0;
	LanceStructuralEncoding structural_encoding = LANCE_STRUCTURAL_ENCODING_DEFAULT;

	bool operator==(const ColumnEncodingOptions &other) const {
		return std::tie(column, compression, has_compression_level, compression_level, structural_encoding) ==
		       std::tie(other.column, other.compression, other.has_compression_level, other.compression_level,
		                other.structural_encoding);
	}

	// The result points into this object's strings.
	LanceColumnEncoding ToFfi() const {
		LanceColumnEncoding result;
		result.column = column.c_str();
		result.compression = compression.empty() ? nullptr : compression.c_str();
		result.has_compression_level = has_compression_level;
		result.compression_level = compression_level;
		result.structural_encoding = structural_encoding;
		return result;
	}
};

struct LanceBindData : public FunctionData {
	LanceBindData() {
		const auto defaults = lance_write_default_config();
		blob_inline_size_threshold = defaults.blob_inline_size_threshold;
		blob_dedicated_size_threshold = defaults.blob_dedicated_size_threshold;
		target_file_size = defaults.target_file_size;
	}

	// Source schema
	vector<Identifier> names;
	vector<LogicalType> types;
	ClientProperties properties;
	// Write mode and destination
	LanceWriteMode mode = LANCE_WRITE_MODE_CREATE;
	bool s3 = false;
	// Storage configuration
	int64_t blob_inline_size_threshold;
	int64_t blob_dedicated_size_threshold;
	int64_t target_file_size;
	string storage_version;
	// Per-column encoding settings, at most one entry per column
	vector<ColumnEncodingOptions> column_encodings;
	// Blob columns
	vector<Identifier> blob_columns;
	// Indexes; index_kinds[i] applies to index_columns[i]
	vector<Identifier> index_columns;
	vector<LanceIndexKind> index_kinds;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<LanceBindData>();
		result->names = names;
		result->types = types;
		result->properties = properties;
		result->mode = mode;
		result->s3 = s3;
		result->blob_inline_size_threshold = blob_inline_size_threshold;
		result->blob_dedicated_size_threshold = blob_dedicated_size_threshold;
		result->target_file_size = target_file_size;
		result->storage_version = storage_version;
		result->column_encodings = column_encodings;
		result->blob_columns = blob_columns;
		result->index_columns = index_columns;
		result->index_kinds = index_kinds;
		return result;
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<LanceBindData>();
		return names == other.names && types == other.types && mode == other.mode && s3 == other.s3 &&
		       blob_inline_size_threshold == other.blob_inline_size_threshold &&
		       blob_dedicated_size_threshold == other.blob_dedicated_size_threshold &&
		       target_file_size == other.target_file_size && storage_version == other.storage_version &&
		       column_encodings == other.column_encodings && blob_columns == other.blob_columns &&
		       index_columns == other.index_columns && index_kinds == other.index_kinds;
	}
};

struct LanceGlobalState : public GlobalFunctionData {
	LanceConversionDataset *dataset = nullptr;
	// Receives every batch, in batch index order, under BATCH_COPY_TO_FILE.
	LanceConversionWriter *ordered_writer = nullptr;
	~LanceGlobalState() override {
		lance_conversion_writer_destroy(ordered_writer);
		lance_conversion_destroy(dataset);
	}
};

// REGULAR_COPY_TO_FILE and PARALLEL_COPY_TO_FILE give each sinking thread its own writer.
struct LanceLocalState : public LocalFunctionData {
	LanceConversionWriter *writer = nullptr;
	~LanceLocalState() override {
		lance_conversion_writer_destroy(writer);
	}
};

// Stays in DuckDB's buffer-managed collection until flushed, so pending batches can spill.
struct LancePreparedBatch : public PreparedBatchData {
	unique_ptr<ColumnDataCollection> collection;
};

LanceConversionWriter *OpenLanceWriter(LanceConversionDataset *dataset) {
	LanceConversionWriter *writer = nullptr;
	ThrowIfLanceError(lance_conversion_writer_open(dataset, &writer), "Lance conversion");
	return writer;
}

void PushLanceChunk(LanceConversionWriter *writer, const LanceBindData &bind, DataChunk &chunk) {
	ArrowArrayWrapper array;
	ArrowConverter::ToArrowArray(chunk, &array.arrow_array, bind.properties, {});
	ThrowIfLanceError(lance_conversion_push(writer, &array.arrow_array), "Lance conversion");
}

void ValidateDuckDBType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::UUID:
	case LogicalTypeId::ENUM:
		return;
	case LogicalTypeId::LIST:
		ValidateDuckDBType(ListType::GetChildType(type));
		return;
	case LogicalTypeId::MAP:
		ValidateDuckDBType(MapType::KeyType(type));
		ValidateDuckDBType(MapType::ValueType(type));
		return;
	case LogicalTypeId::ARRAY:
		ValidateDuckDBType(ArrayType::GetChildType(type));
		return;
	case LogicalTypeId::STRUCT:
		for (const auto &child : StructType::GetChildTypes(type)) {
			ValidateDuckDBType(child.second);
		}
		return;
	default:
		throw NotImplementedException("FORMAT LANCE does not support %s; cast it explicitly", type.ToString());
	}
}

int64_t ParseSizeOption(ClientContext &context, const Identifier &name, const vector<Value> &values) {
	if (values.size() != 1 || values[0].IsNull()) {
		throw BinderException("%s requires one non-negative integer", name);
	}
	auto value = values[0].CastAs(context, LogicalType::BIGINT).GetValue<int64_t>();
	if (value < 0) {
		throw BinderException("%s must be non-negative", name);
	}
	return value;
}

using ColumnIndexMap = identifier_map_t<idx_t>;

ColumnIndexMap BuildColumnIndexMap(const vector<Identifier> &names) {
	ColumnIndexMap result;
	for (idx_t index = 0; index < names.size(); index++) {
		result.emplace(names[index], index);
	}
	return result;
}

vector<Identifier> ParseBlobColumns(ClientContext &context, const vector<Value> &values,
                                    const vector<Identifier> &names, const vector<LogicalType> &types,
                                    const ColumnIndexMap &column_indexes) {
	if (values.empty()) {
		throw BinderException("BLOB_COLUMNS requires at least one column");
	}
	vector<Identifier> result;
	identifier_set_t seen;
	for (const auto &value : values) {
		if (value.IsNull()) {
			throw BinderException("BLOB_COLUMNS cannot contain NULL");
		}
		auto requested = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		auto entry = column_indexes.find(Identifier(requested));
		if (entry == column_indexes.end()) {
			throw BinderException("BLOB_COLUMNS column not found: %s", requested);
		}
		auto index = entry->second;
		if (types[index].id() != LogicalTypeId::VARCHAR) {
			throw BinderException("BLOB_COLUMNS column must be VARCHAR: %s", names[index]);
		}
		if (!seen.insert(names[index]).second) {
			throw BinderException("Duplicate BLOB_COLUMNS column: %s", names[index]);
		}
		result.push_back(names[index]);
	}
	return result;
}

struct IndexOptionSpec {
	std::string_view name;
	LanceIndexKind kind;
};

constexpr IndexOptionSpec INDEX_OPTION_SPECS[] = {
    {"scalar_index_columns", LANCE_INDEX_SCALAR},
    {"vector_index_columns", LANCE_INDEX_VECTOR},
    {"text_index_columns", LANCE_INDEX_TEXT},
    {"bloom_filter_index_columns", LANCE_INDEX_BLOOM_FILTER},
};

const IndexOptionSpec *FindIndexOption(const Identifier &option_name) {
	for (const auto &spec : INDEX_OPTION_SPECS) {
		if (option_name == Identifier(spec.name)) {
			return &spec;
		}
	}
	return nullptr;
}

// Appends each listed column, with the option's index kind, to the bind data's indexes.
void ParseIndexColumns(ClientContext &context, const Identifier &option_name, LanceIndexKind kind,
                       const vector<Value> &values, const vector<Identifier> &names,
                       const ColumnIndexMap &column_indexes, LanceBindData &data) {
	if (values.empty()) {
		throw BinderException("%s requires at least one column", option_name);
	}
	identifier_set_t seen;
	for (const auto &value : values) {
		if (value.IsNull()) {
			throw BinderException("%s cannot contain NULL", option_name);
		}
		auto requested = value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		auto entry = column_indexes.find(Identifier(requested));
		if (entry == column_indexes.end()) {
			throw BinderException("%s column not found: %s", option_name, requested);
		}
		auto index = entry->second;
		if (!seen.insert(names[index]).second) {
			throw BinderException("Duplicate index column: %s", names[index]);
		}
		data.index_columns.push_back(names[index]);
		data.index_kinds.push_back(kind);
	}
}

ColumnEncodingOptions &FindOrAddColumnEncoding(vector<ColumnEncodingOptions> &encodings, const Identifier &column) {
	for (auto &encoding : encodings) {
		if (encoding.column == column) {
			return encoding;
		}
	}
	encodings.emplace_back();
	encodings.back().column = column;
	return encodings.back();
}

// Resolve nested struct fields, traversing list elements implicitly. Exact
// top-level names take precedence, including column names containing dots.
Identifier ResolveEncodingColumn(const Identifier &option_name, const Identifier &requested,
                                 const vector<Identifier> &names, const vector<LogicalType> &types,
                                 const ColumnIndexMap &indexes) {
	auto exact = indexes.find(Identifier(requested));
	if (exact != indexes.end()) {
		return names[exact->second];
	}
	auto parts = StringUtil::Split(requested.GetIdentifierName(), '.');
	if (parts.empty() || parts[0].empty()) {
		throw BinderException("%s column not found: %s", option_name, requested);
	}
	auto root = indexes.find(Identifier(parts[0]));
	if (root == indexes.end()) {
		throw BinderException("%s column not found: %s", option_name, requested);
	}
	string canonical = names[root->second].GetIdentifierName();
	const LogicalType *type = &types[root->second];
	for (idx_t index = 1; index < parts.size(); index++) {
		while (type->id() == LogicalTypeId::LIST || type->id() == LogicalTypeId::ARRAY) {
			type = type->id() == LogicalTypeId::LIST ? &ListType::GetChildType(*type) : &ArrayType::GetChildType(*type);
		}
		if (type->id() != LogicalTypeId::STRUCT) {
			throw BinderException("%s column not found: %s", option_name, requested);
		}
		const auto &children = StructType::GetChildTypes(*type);
		auto child = std::find_if(children.begin(), children.end(), [&](const auto &entry) {
			return entry.first == Identifier(parts[index]);
		});
		if (child == children.end()) {
			throw BinderException("%s column not found: %s", option_name, requested);
		}
		canonical += "." + child->first.GetIdentifierName();
		type = &child->second;
	}
	return Identifier(canonical);
}

// Parses a struct option such as {'column': value}, applying each value to its column's encoding settings.
template <class APPLY>
void ParseColumnSettings(const Identifier &option_name, const string &example, const string &value_name,
                         const vector<Value> &values, const vector<Identifier> &names, const vector<LogicalType> &types,
                         const ColumnIndexMap &column_indexes, vector<ColumnEncodingOptions> &encodings, APPLY apply) {
	if (values.size() != 1 || values[0].type().id() != LogicalTypeId::STRUCT || values[0].IsNull()) {
		throw BinderException("%s requires a struct such as %s", option_name, example);
	}
	auto &entries = StructType::GetChildTypes(values[0].type());
	auto &children = StructValue::GetChildren(values[0]);
	identifier_set_t seen;
	for (idx_t i = 0; i < entries.size(); i++) {
		const auto &requested = entries[i].first;
		auto column = ResolveEncodingColumn(option_name, requested, names, types, column_indexes);
		if (option_name == "COLUMN_STRUCTURAL_ENCODING" && column_indexes.find(column) == column_indexes.end()) {
			throw BinderException("COLUMN_STRUCTURAL_ENCODING applies to top-level columns, not nested fields");
		}
		if (!seen.insert(column).second) {
			throw BinderException("Duplicate %s column: %s", option_name, column);
		}
		if (children[i].IsNull()) {
			throw BinderException("%s %s cannot be NULL: %s", option_name, value_name, column);
		}
		apply(FindOrAddColumnEncoding(encodings, column), children[i]);
	}
}

string ParseStringSetting(ClientContext &context, const Value &value) {
	return value.CastAs(context, LogicalType::VARCHAR).GetValue<string>();
}

string ParseCompression(ClientContext &context, const Value &value, const Identifier &column) {
	auto compression = ParseStringSetting(context, value);
	if (compression.empty()) {
		throw BinderException("COLUMN_COMPRESSION algorithm cannot be empty: %s", column);
	}
	return compression;
}

LanceStructuralEncoding ParseStructuralEncoding(ClientContext &context, const Value &value, const Identifier &column) {
	auto encoding = ParseStringSetting(context, value);
	if (StringUtil::CIEquals(encoding, "miniblock")) {
		return LANCE_STRUCTURAL_ENCODING_MINIBLOCK;
	}
	if (StringUtil::CIEquals(encoding, "fullzip")) {
		return LANCE_STRUCTURAL_ENCODING_FULLZIP;
	}
	throw BinderException("COLUMN_STRUCTURAL_ENCODING encoding must be 'miniblock' or 'fullzip': %s", column);
}

// Lance silently ignores compression levels it cannot parse, so reject them here.
int32_t ParseCompressionLevel(const Value &value, const Identifier &column) {
	auto level = value.DefaultTryCastAs(LogicalType::INTEGER);
	if (!value.type().IsIntegral() || !level) {
		throw BinderException("COLUMN_COMPRESSION_LEVEL level must be an integer: %s", column);
	}
	return level->GetValue<int32_t>();
}

void ValidateIndexColumns(const LanceBindData &data) {
	identifier_set_t blob_columns(data.blob_columns.begin(), data.blob_columns.end());
	identifier_set_t indexed;
	for (const auto &column : data.index_columns) {
		if (!indexed.insert(column).second) {
			throw BinderException("Column cannot have multiple indexes in one COPY: %s", column);
		}
		if (blob_columns.count(column)) {
			throw BinderException("BLOB_COLUMNS column cannot also be indexed: %s", column);
		}
	}
}

unique_ptr<FunctionData> LanceBind(ClientContext &context, CopyFunctionBindInput &input,
                                   const vector<Identifier> &names, const vector<LogicalType> &types) {
	auto result = make_uniq<LanceBindData>();
	result->names = names;
	result->types = types;
	result->properties = context.GetClientProperties();
	result->properties.arrow_use_list_view = false;
	result->properties.produce_arrow_string_view = false;
	result->properties.arrow_lossless_conversion = false;
	result->properties.arrow_offset_size = ArrowOffsetSize::LARGE;
	result->properties.arrow_output_version = ArrowFormatVersion::V1_0;
	for (const auto &type : types) {
		ValidateDuckDBType(type);
	}
	auto column_indexes = BuildColumnIndexMap(names);
	bool has_write_mode = false;
	for (auto &option : input.info.options) {
		if (option.first == "overwrite" || option.first == "append") {
			if (has_write_mode) {
				throw BinderException("Only one of OVERWRITE or APPEND can be specified");
			}
			has_write_mode = true;
			if (option.second.size() > 1 || (!option.second.empty() && option.second[0].IsNull())) {
				throw BinderException("%s requires a boolean", option.first);
			}
			auto enabled =
			    option.second.empty() || option.second[0].CastAs(context, LogicalType::BOOLEAN).GetValue<bool>();
			if (enabled) {
				result->mode = option.first == "overwrite" ? LANCE_WRITE_MODE_OVERWRITE : LANCE_WRITE_MODE_APPEND;
			}
		} else if (option.first == "blob_inline_size_threshold") {
			result->blob_inline_size_threshold = ParseSizeOption(context, option.first, option.second);
		} else if (option.first == "blob_dedicated_size_threshold") {
			result->blob_dedicated_size_threshold = ParseSizeOption(context, option.first, option.second);
			if (result->blob_dedicated_size_threshold == 0) {
				throw BinderException("BLOB_DEDICATED_SIZE_THRESHOLD must be greater than zero");
			}
		} else if (option.first == "target_file_size") {
			result->target_file_size = ParseSizeOption(context, option.first, option.second);
			if (result->target_file_size == 0) {
				throw BinderException("TARGET_FILE_SIZE must be greater than zero");
			}
		} else if (option.first == "storage_version") {
			if (option.second.size() != 1 || option.second[0].IsNull()) {
				throw BinderException("STORAGE_VERSION requires one version string");
			}
			result->storage_version = option.second[0].CastAs(context, LogicalType::VARCHAR).GetValue<string>();
		} else if (option.first == "column_compression") {
			ParseColumnSettings("COLUMN_COMPRESSION", /*example=*/"{'column': 'zstd'}", /*value_name=*/"algorithm",
			                    option.second, names, types, column_indexes, result->column_encodings,
			                    [&](ColumnEncodingOptions &encoding, const Value &value) {
				                    encoding.compression = ParseCompression(context, value, encoding.column);
			                    });
		} else if (option.first == "column_compression_level") {
			ParseColumnSettings("COLUMN_COMPRESSION_LEVEL", /*example=*/"{'column': 3}", /*value_name=*/"level",
			                    option.second, names, types, column_indexes, result->column_encodings,
			                    [](ColumnEncodingOptions &encoding, const Value &value) {
				                    encoding.has_compression_level = true;
				                    encoding.compression_level = ParseCompressionLevel(value, encoding.column);
			                    });
		} else if (option.first == "column_structural_encoding") {
			ParseColumnSettings("COLUMN_STRUCTURAL_ENCODING", /*example=*/"{'column': 'miniblock'}",
			                    /*value_name=*/"encoding", option.second, names, types, column_indexes,
			                    result->column_encodings, [&](ColumnEncodingOptions &encoding, const Value &value) {
				                    encoding.structural_encoding =
				                        ParseStructuralEncoding(context, value, encoding.column);
			                    });
		} else if (option.first == "blob_columns") {
			result->blob_columns = ParseBlobColumns(context, option.second, names, types, column_indexes);
		} else if (auto index_option = FindIndexOption(option.first)) {
			ParseIndexColumns(context, option.first, index_option->kind, option.second, names, column_indexes, *result);
		} else {
			throw BinderException("Unsupported option for FORMAT LANCE: %s", option.first);
		}
	}
	ValidateIndexColumns(*result);
	auto &fs = FileSystem::GetFileSystem(context);
	if (FileSystem::IsRemoteFile(input.info.file_path)) {
		if (!StringUtil::CIStartsWith(input.info.file_path, "s3://")) {
			throw BinderException("FORMAT LANCE supports only local and s3:// destinations");
		}
		result->s3 = true;
		return result;
	}
	if (result->mode == LANCE_WRITE_MODE_CREATE && (fs.FileExists(fs.ExpandPath(input.info.file_path)) ||
	                                                fs.DirectoryExists(fs.ExpandPath(input.info.file_path)))) {
		throw IOException("Lance destination must not exist: %s", input.info.file_path);
	}
	return result;
}

unique_ptr<GlobalFunctionData> LanceInitialize(ClientContext &context, FunctionData &bind_p, const string &path) {
	auto &bind = bind_p.Cast<LanceBindData>();
	ArrowSchemaWrapper schema;
	ArrowConverter::ToArrowSchema(&schema.arrow_schema, bind.types, IdentifiersToStrings(bind.names), bind.properties);
	ExportSourceMetadata(bind.types, schema.arrow_schema);
	auto state = make_uniq<LanceGlobalState>();
	auto write_config = lance_write_default_config();
	write_config.mode = bind.mode;
	write_config.blob_inline_size_threshold = bind.blob_inline_size_threshold;
	write_config.blob_dedicated_size_threshold = bind.blob_dedicated_size_threshold;
	write_config.target_file_size = bind.target_file_size;
	write_config.storage_version = bind.storage_version.empty() ? nullptr : bind.storage_version.c_str();
	auto string_pointers = [](const vector<Identifier> &columns) {
		vector<const char *> result;
		for (const auto &column : columns) {
			result.push_back(column.c_str());
		}
		return result;
	};
	auto blob_columns = string_pointers(bind.blob_columns);
	write_config.blob_columns = blob_columns.data();
	write_config.blob_column_count = blob_columns.size();

	vector<LanceColumnEncoding> column_encodings;
	for (const auto &encoding : bind.column_encodings) {
		column_encodings.push_back(encoding.ToFfi());
	}
	write_config.column_encodings = column_encodings.data();
	write_config.column_encoding_count = column_encodings.size();

	auto index_columns = string_pointers(bind.index_columns);
	write_config.index_columns = index_columns.data();
	write_config.index_kinds = bind.index_kinds.data();
	write_config.index_column_count = index_columns.size();

	unique_ptr<LanceS3Options> s3;
	// S3 blob URIs are read with the destination's S3 secret, or the unscoped one for a local destination.
	if (bind.s3 || !bind.blob_columns.empty()) {
		s3 = ReadS3Options(context, bind.s3 ? path : "s3://");
	}
	auto opendal_config = ReadOpendalConfig(context);
	ThrowIfLanceError(lance_conversion_open(path.c_str(), &schema.arrow_schema, &write_config,
	                                        s3 ? &s3->config : nullptr, &opendal_config, &state->dataset),
	                  "Lance conversion");
	return state;
}

unique_ptr<LocalFunctionData> LanceLocalInitialize(ExecutionContext &context, FunctionData &bind) {
	return make_uniq<LanceLocalState>();
}

void LanceSinkChunk(ExecutionContext &context, FunctionData &bind_p, GlobalFunctionData &global_p,
                    LocalFunctionData &local_p, DataChunk &input) {
	auto &bind = bind_p.Cast<LanceBindData>();
	auto &local = local_p.Cast<LanceLocalState>();
	if (!local.writer) {
		local.writer = OpenLanceWriter(global_p.Cast<LanceGlobalState>().dataset);
	}
	PushLanceChunk(local.writer, bind, input);
}

void LanceCombine(ExecutionContext &context, FunctionData &bind, GlobalFunctionData &global,
                  LocalFunctionData &local_p) {
	auto &local = local_p.Cast<LanceLocalState>();
	if (local.writer) {
		ThrowIfLanceError(lance_conversion_writer_finish(local.writer), "Lance conversion");
	}
}

unique_ptr<PreparedBatchData> LancePrepareBatch(ClientContext &context, FunctionData &bind, GlobalFunctionData &global,
                                                unique_ptr<ColumnDataCollection> collection) {
	auto result = make_uniq<LancePreparedBatch>();
	result->collection = std::move(collection);
	return result;
}

// DuckDB calls this for one batch at a time, in batch index order.
void LanceFlushBatch(ClientContext &context, FunctionData &bind_p, GlobalFunctionData &global_p,
                     PreparedBatchData &batch_p) {
	auto &bind = bind_p.Cast<LanceBindData>();
	auto &global = global_p.Cast<LanceGlobalState>();
	auto &batch = batch_p.Cast<LancePreparedBatch>();
	if (!global.ordered_writer) {
		global.ordered_writer = OpenLanceWriter(global.dataset);
	}
	for (auto &chunk : batch.collection->Chunks()) {
		PushLanceChunk(global.ordered_writer, bind, chunk);
	}
}

void LanceFinalize(ClientContext &context, FunctionData &bind, GlobalFunctionData &global_p) {
	auto &global = global_p.Cast<LanceGlobalState>();
	if (global.ordered_writer) {
		ThrowIfLanceError(lance_conversion_writer_finish(global.ordered_writer), "Lance conversion");
	}
	ThrowIfLanceError(lance_conversion_finish(global.dataset), "Lance conversion");
	LanceWriteMetrics metrics;
	lance_conversion_get_metrics(global.dataset, &metrics);
	auto &profiler = QueryProfiler::Get(context);
	profiler.TrackBytesRead(metrics.bytes_read);
	profiler.TrackBytesWritten(metrics.bytes_written);
}

CopyFunctionExecutionMode LanceExecutionMode(bool preserve_insertion_order, bool supports_batch_index) {
	if (!preserve_insertion_order) {
		return CopyFunctionExecutionMode::PARALLEL_COPY_TO_FILE;
	}
	if (supports_batch_index) {
		return CopyFunctionExecutionMode::BATCH_COPY_TO_FILE;
	}
	return CopyFunctionExecutionMode::REGULAR_COPY_TO_FILE;
}

CopyFunction MakeLanceCopyFunction() {
	CopyFunction function("lance");
	function.extension = "lance";
	function.copy_to_bind = LanceBind;
	function.copy_to_initialize_global = LanceInitialize;
	function.copy_to_initialize_local = LanceLocalInitialize;
	function.copy_to_sink = LanceSinkChunk;
	function.copy_to_combine = LanceCombine;
	function.copy_to_finalize = LanceFinalize;
	function.prepare_batch = LancePrepareBatch;
	function.flush_batch = LanceFlushBatch;
	function.execution_mode = LanceExecutionMode;
	return function;
}

enum class SampleKind : uint8_t { PERCENT, ROWS };

struct SampleOptionSpec {
	std::string_view name;
	SampleKind kind;
};

constexpr SampleOptionSpec SAMPLE_OPTION_SPECS[] = {
    {"sample_percent", SampleKind::PERCENT},
    {"sample_rows", SampleKind::ROWS},
};

Value ParseSampleSize(ClientContext &context, SampleKind kind, const vector<Value> &values) {
	switch (kind) {
	case SampleKind::PERCENT: {
		auto value = values[0].CastAs(context, LogicalType::DOUBLE).GetValue<double>();
		if (std::isnan(value) || value < 0 || value > 100) {
			throw InvalidInputException("SAMPLE_PERCENT must be between 0 and 100");
		}
		return Value::DOUBLE(value);
	}
	case SampleKind::ROWS: {
		auto value = values[0].CastAs(context, LogicalType::BIGINT).GetValue<int64_t>();
		if (value < 0) {
			throw InvalidInputException("SAMPLE_ROWS must be non-negative");
		}
		const idx_t max_sample_rows = SampleOptions::MAX_SAMPLE_ROWS;
		if (NumericCast<idx_t>(value) > max_sample_rows) {
			throw InvalidInputException("SAMPLE_ROWS must not exceed %llu", max_sample_rows);
		}
		return Value::BIGINT(value);
	}
	}
	throw InternalException("Unknown sample kind");
}

SampleMethod GetSampleMethod(SampleKind kind) {
	switch (kind) {
	case SampleKind::PERCENT:
		return SampleMethod::BERNOULLI_SAMPLE;
	case SampleKind::ROWS:
		return SampleMethod::RESERVOIR_SAMPLE;
	}
	throw InternalException("Unknown sample kind");
}

// Removes SAMPLE_PERCENT / SAMPLE_ROWS from the COPY options and returns the equivalent DuckDB sample.
unique_ptr<SampleOptions> TakeSampleOptions(ClientContext &context, CopyInfo &info) {
	unique_ptr<SampleOptions> sample;
	for (const auto &spec : SAMPLE_OPTION_SPECS) {
		auto entry = info.options.find(Identifier(spec.name));
		if (entry == info.options.end()) {
			continue;
		}
		if (sample) {
			throw BinderException("Only one of SAMPLE_PERCENT or SAMPLE_ROWS can be specified");
		}
		if (entry->second.size() != 1 || entry->second[0].IsNull()) {
			throw BinderException("%s requires one value", StringUtil::Upper(string(spec.name)));
		}
		sample = make_uniq<SampleOptions>();
		sample->sample_size = ParseSampleSize(context, spec.kind, entry->second);
		sample->is_percentage = spec.kind == SampleKind::PERCENT;
		sample->method = GetSampleMethod(spec.kind);
		info.options.erase(entry);
	}
	return sample;
}

unique_ptr<QueryNode> ApplySample(unique_ptr<QueryNode> query, unique_ptr<SampleOptions> sample) {
	auto subquery = make_uniq<SelectStatement>();
	subquery->node = std::move(query);
	auto result = make_uniq<SelectNode>();
	result->select_list.push_back(make_uniq<StarExpression>());
	result->from_table = make_uniq<SubqueryRef>(std::move(subquery));
	result->sample = std::move(sample);
	return result;
}

// Own the plan so DuckDB file rotation/overwrite options cannot operate on a dataset directory.
BoundStatement LancePlan(Binder &binder, CopyStatement &statement) {
	auto function = MakeLanceCopyFunction();
	auto info = statement.info->Copy();
	auto query = info->select_statement->Copy();
	auto sample = TakeSampleOptions(binder.context, *info);
	if (sample) {
		query = ApplySample(std::move(query), std::move(sample));
	}
	auto source = binder.Bind(*query);
	QueryResult::DeduplicateColumns(source.names);
	CopyFunctionBindInput input(*info);
	auto data = LanceBind(binder.context, input, source.names, source.types);
	auto copy = make_uniq<LogicalCopyToFile>(function, std::move(data), std::move(info), binder.GenerateTableIndex());
	copy->file_path = statement.info->file_path;
	copy->file_extension = "lance";
	copy->use_tmp_file = false;
	copy->overwrite_mode = CopyOverwriteMode::COPY_ERROR_ON_CONFLICT;
	copy->per_thread_output = false;
	copy->rotate = false;
	copy->partition_output = false;
	copy->write_partition_columns = false;
	copy->return_type = CopyFunctionReturnType::CHANGED_ROWS;
	copy->names = source.names;
	copy->expected_types = source.types;
	copy->AddChild(std::move(source.plan));
	BoundStatement result;
	result.names = GetCopyFunctionReturnNames(copy->return_type);
	result.types = GetCopyFunctionReturnLogicalTypes(copy->return_type);
	result.plan = std::move(copy);
	return result;
}

} // namespace

void RegisterLanceCopyFunction(ExtensionLoader &loader) {
	auto function = MakeLanceCopyFunction();
	function.plan = LancePlan;
	loader.RegisterFunction(function);
}

} // namespace duckdb
