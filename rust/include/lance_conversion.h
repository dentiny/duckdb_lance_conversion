#pragma once

#include <stddef.h>
#include <stdint.h>

struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;
struct LanceConversionDataset;
struct LanceConversionWriter;
struct HuggingFaceStreamFactory;
struct WarcStreamFactory;

struct LanceS3Config {
	const char *endpoint = NULL;
	const char *region = NULL;
	const char *key_id = NULL;
	const char *secret = NULL;
	const char *session_token = NULL;
	int32_t use_ssl = 0;
	int32_t virtual_host_style = 0;
};

enum LanceWriteMode : int32_t {
	LANCE_WRITE_MODE_CREATE = 0,
	LANCE_WRITE_MODE_APPEND = 1,
	LANCE_WRITE_MODE_OVERWRITE = 2,
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
	// Blob columns.
	const char *const *blob_columns = NULL;
	size_t blob_column_count = 0;
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

// NULL means success. Free each owned error with lance_conversion_error_free.
void lance_conversion_error_free(char *error);
// A dataset write collects the fragments of any number of writers and commits
// them as one dataset version in lance_conversion_finish.
char *lance_conversion_open(const char *path, const struct ArrowSchema *schema, const struct LanceWriteConfig *config,
                            const struct LanceS3Config *s3, struct LanceConversionDataset **output);
char *lance_conversion_finish(struct LanceConversionDataset *dataset);
void lance_conversion_get_metrics(const struct LanceConversionDataset *dataset, struct LanceWriteMetrics *output);
// Never throws across the ABI.
void lance_conversion_destroy(struct LanceConversionDataset *dataset);

// Writers write data files for one dataset write, so several threads can write
// in parallel; a writer itself is not thread-safe.
// Safe to call concurrently on one dataset until lance_conversion_finish.
char *lance_conversion_writer_open(const struct LanceConversionDataset *dataset, struct LanceConversionWriter **output);
// Takes ownership of array on import and clears its release callback.
char *lance_conversion_push(struct LanceConversionWriter *writer, struct ArrowArray *array);
// Adds the writer's fragments to the dataset commit, in call order.
char *lance_conversion_writer_finish(struct LanceConversionWriter *writer);
// Aborts an unfinished writer; its fragments are not committed. Never throws across the ABI.
void lance_conversion_writer_destroy(struct LanceConversionWriter *writer);

char *lance_huggingface_open(const char *dataset, const char *config, const char *split, const char *token,
                             int32_t preserve_insertion_order, uint64_t max_read_parallelism,
                             struct HuggingFaceStreamFactory **output);
char *lance_huggingface_get_schema(const struct HuggingFaceStreamFactory *factory, struct ArrowSchema *output);
char *lance_huggingface_get_stream(struct HuggingFaceStreamFactory *factory, struct ArrowArrayStream *output);
void lance_huggingface_get_metrics(const struct HuggingFaceStreamFactory *factory, struct LanceReadMetrics *output);
void lance_huggingface_destroy(struct HuggingFaceStreamFactory *factory);

char *lance_warc_open(const char *path, const char *index_path, const struct LanceS3Config *s3,
                      uint64_t max_read_parallelism, struct WarcStreamFactory **output);
char *lance_warc_get_schema(const struct WarcStreamFactory *factory, struct ArrowSchema *output);
char *lance_warc_get_stream(const struct WarcStreamFactory *factory, const char *const *columns, size_t column_count,
                            struct ArrowArrayStream *output);
void lance_warc_get_metrics(const struct WarcStreamFactory *factory, struct LanceReadMetrics *output);
void lance_warc_destroy(struct WarcStreamFactory *factory);

#ifdef __cplusplus
}
#endif
