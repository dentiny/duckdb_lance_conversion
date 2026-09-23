#pragma once

#include <stddef.h>
#include <stdint.h>

struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;
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

#ifdef __cplusplus
extern "C" {
#endif

// NULL means success. Free each owned error with lance_conversion_error_free.
void lance_conversion_error_free(char *error);
char *lance_conversion_open(const char *path, const struct ArrowSchema *schema,
                            const struct LanceWriteConfig *config, const struct LanceS3Config *s3,
                            struct LanceConversionWriter **output);
// Takes ownership of array on import and clears its release callback.
char *lance_conversion_push(struct LanceConversionWriter *writer, struct ArrowArray *array);
char *lance_conversion_finish(struct LanceConversionWriter *writer);
// Aborts unfinished writes. Never throws across the ABI.
void lance_conversion_destroy(struct LanceConversionWriter *writer);

char *lance_huggingface_open(const char *dataset, const char *config, const char *split, const char *token,
                             struct HuggingFaceStreamFactory **output);
char *lance_huggingface_get_schema(const struct HuggingFaceStreamFactory *factory, struct ArrowSchema *output);
char *lance_huggingface_get_stream(struct HuggingFaceStreamFactory *factory, struct ArrowArrayStream *output);
void lance_huggingface_destroy(struct HuggingFaceStreamFactory *factory);

char *lance_warc_open(const char *path, const struct LanceS3Config *s3, struct WarcStreamFactory **output);
char *lance_warc_get_schema(const struct WarcStreamFactory *factory, struct ArrowSchema *output);
char *lance_warc_get_stream(const struct WarcStreamFactory *factory, const char *const *columns, size_t column_count,
                            struct ArrowArrayStream *output);
void lance_warc_destroy(struct WarcStreamFactory *factory);

#ifdef __cplusplus
}
#endif
