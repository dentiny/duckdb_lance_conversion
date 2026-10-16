#pragma once

#include <stddef.h>
#include <stdint.h>

struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;
struct LanceConversionDataset;
struct LanceConversionWriter;
struct SourceStreamFactory;

struct LanceS3Config {
	const char *endpoint = NULL;
	const char *region = NULL;
	const char *key_id = NULL;
	const char *secret = NULL;
	const char *session_token = NULL;
	int32_t use_ssl = 0;
	int32_t virtual_host_style = 0;
};

// Snapshot of the connection's OpenDAL extension settings. Durations are milliseconds.
struct LanceOpendalConfig {
	uint64_t timeout_ms;
	uint64_t io_timeout_ms;
	uint64_t retry_max_times;
	uint64_t retry_min_delay_ms;
	uint64_t retry_max_delay_ms;
	double retry_factor;
};

enum LanceWriteMode : int32_t {
	LANCE_WRITE_MODE_CREATE = 0,
	LANCE_WRITE_MODE_APPEND = 1,
	LANCE_WRITE_MODE_OVERWRITE = 2,
};

// What to do with a blob column value whose object is missing or forbidden.
// Other read errors, such as rate limits, always fail the write.
enum LanceBlobOnError : int32_t {
	LANCE_BLOB_ON_ERROR_FAIL = 0,
	LANCE_BLOB_ON_ERROR_NULL = 1,
	LANCE_BLOB_ON_ERROR_SKIP = 2,
};

struct LanceWriteConfig {
	// Write mode.
	LanceWriteMode mode = LANCE_WRITE_MODE_CREATE;
	// Storage configurations.
	int64_t blob_inline_size_threshold = 2 * 1024 * 1024;
	int64_t blob_dedicated_size_threshold = 16 * 1024 * 1024;
	int64_t target_file_size = 512 * 1024 * 1024;
	// Lance file format version such as "2.1" or "stable"; NULL or empty uses "stable".
	const char *storage_version = NULL;
	// Per-column Lance compression algorithms, passed to Lance as-is.
	const char *const *compression_columns = NULL;
	const char *const *compression_algorithms = NULL;
	size_t compression_column_count = 0;
	// Per-column Lance compression levels.
	const char *const *compression_level_columns = NULL;
	const int32_t *compression_levels = NULL;
	size_t compression_level_column_count = 0;
	// Per-column Lance structural encodings such as "miniblock" or "fullzip", passed to Lance as-is.
	const char *const *structural_encoding_columns = NULL;
	const char *const *structural_encodings = NULL;
	size_t structural_encoding_column_count = 0;
	// Blob columns.
	const char *const *blob_columns = NULL;
	size_t blob_column_count = 0;
	LanceBlobOnError blob_on_error = LANCE_BLOB_ON_ERROR_FAIL;
	// Scalar index columns.
	const char *const *scalar_index_columns = NULL;
	size_t scalar_index_column_count = 0;
	// Vector index columns.
	const char *const *vector_index_columns = NULL;
	size_t vector_index_column_count = 0;
	// Text index columns.
	const char *const *text_index_columns = NULL;
	size_t text_index_column_count = 0;
	// Bloom filter index columns.
	const char *const *bloom_filter_index_columns = NULL;
	size_t bloom_filter_index_column_count = 0;
};

// Cumulative source metrics sampled while a scan is running.
// bytes_read contributes to DuckDB's query-level TOTAL_BYTES_READ.
// total_bytes is the expected source size used only for progress reporting.
struct LanceReadMetrics {
	uint64_t bytes_read = 0;
	uint64_t total_bytes = 0;
};

// Final destination I/O metrics available after lance_conversion_finish.
// Both fields contribute to DuckDB's query-level byte counters; bytes_read
// includes Lance metadata read while opening or updating the destination.
struct LanceWriteMetrics {
	uint64_t bytes_read = 0;
	uint64_t bytes_written = 0;
};

#ifdef __cplusplus
extern "C" {
#endif

// Returns the storage defaults defined in Rust.
struct LanceOpendalConfig lance_opendal_default_config(void);

// NULL means success. Free each owned error with lance_conversion_error_free.
void lance_conversion_error_free(char *error);
// A dataset write collects the fragments of any number of writers and commits
// them as one dataset version in lance_conversion_finish.
char *lance_conversion_open(const char *path, const struct ArrowSchema *schema, const struct LanceWriteConfig *config,
                            const struct LanceS3Config *s3, const struct LanceOpendalConfig *opendal_config,
                            struct LanceConversionDataset **output);
char *lance_conversion_finish(struct LanceConversionDataset *dataset);
void lance_conversion_get_metrics(const struct LanceConversionDataset *dataset, struct LanceWriteMetrics *output);
void lance_conversion_destroy(struct LanceConversionDataset *dataset);

// Writers write data files for one dataset write, so several threads can write
// in parallel; a writer itself is not thread-safe.
// Opens a writer that writes data files in a background task. On success, *output owns the writer; free it
// with lance_conversion_writer_destroy. On error, *output is NULL. Threads may open writers on one dataset
// concurrently, but not concurrently with lance_conversion_finish. Fragments of writers finished after
// lance_conversion_finish are never committed.
char *lance_conversion_writer_open(const struct LanceConversionDataset *dataset, struct LanceConversionWriter **output);
// Takes ownership of array on import and clears its release callback.
char *lance_conversion_push(struct LanceConversionWriter *writer, struct ArrowArray *array);
// Waits for the writer's pending data files, then adds its fragments to the dataset commit, in call order.
// Returns NULL on success; on error, none of this writer's rows are committed. Call at most once per writer.
char *lance_conversion_writer_finish(struct LanceConversionWriter *writer);
// Aborts an unfinished writer; its fragments are not committed. Never throws across the ABI.
void lance_conversion_writer_destroy(struct LanceConversionWriter *writer);

// Sources reopened for each scan. Hugging Face opens the dataset here to learn its schema.
char *lance_huggingface_open(const char *dataset, const char *config, const char *split, const char *token,
                             const struct LanceOpendalConfig *opendal_config, int32_t preserve_insertion_order,
                             uint64_t max_read_parallelism, uint64_t read_ahead_bytes,
                             struct SourceStreamFactory **output);
char *lance_warc_open(const char *path, const char *index_path, const struct LanceS3Config *s3,
                      const struct LanceOpendalConfig *opendal_config, uint64_t max_read_parallelism,
                      struct SourceStreamFactory **output);
// format is "json" or "jsonl"; NULL or empty infers it from the path extension.
char *lance_atif_open(const char *path, const char *format, const struct LanceS3Config *s3,
                      const struct LanceOpendalConfig *opendal_config, struct SourceStreamFactory **output);
char *lance_source_get_schema(const struct SourceStreamFactory *factory, struct ArrowSchema *output);
// columns lists the projected column names in output order; column_count 0 reads every column.
char *lance_source_get_stream(const struct SourceStreamFactory *factory, const char *const *columns,
                              size_t column_count, struct ArrowArrayStream *output);
void lance_source_get_metrics(const struct SourceStreamFactory *factory, struct LanceReadMetrics *output);
void lance_source_destroy(struct SourceStreamFactory *factory);

#ifdef __cplusplus
}
#endif
