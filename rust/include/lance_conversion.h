#pragma once

#include <stdint.h>

struct ArrowSchema;
struct ArrowArray;
struct LanceConversionWriter;

#ifdef __cplusplus
#define LANCE_CONVERSION_DEFAULT(value) = value
#else
#define LANCE_CONVERSION_DEFAULT(value)
#endif

struct LanceS3Config {
	const char *endpoint LANCE_CONVERSION_DEFAULT(nullptr);
	const char *region LANCE_CONVERSION_DEFAULT(nullptr);
	const char *key_id LANCE_CONVERSION_DEFAULT(nullptr);
	const char *secret LANCE_CONVERSION_DEFAULT(nullptr);
	const char *session_token LANCE_CONVERSION_DEFAULT(nullptr);
	int32_t use_ssl LANCE_CONVERSION_DEFAULT(1);
	int32_t virtual_host_style LANCE_CONVERSION_DEFAULT(0);
};

#undef LANCE_CONVERSION_DEFAULT

#ifdef __cplusplus
extern "C" {
#endif

// NULL means success. Free each owned error with lance_conversion_error_free.
void lance_conversion_error_free(char *error);
char *lance_conversion_open(const char *path, const struct ArrowSchema *schema, int32_t overwrite,
                            const struct LanceS3Config *s3, struct LanceConversionWriter **output);
// Takes ownership of array on import and clears its release callback.
char *lance_conversion_push(struct LanceConversionWriter *writer, struct ArrowArray *array);
char *lance_conversion_finish(struct LanceConversionWriter *writer);
// Aborts unfinished writes. Never throws across the ABI.
void lance_conversion_destroy(struct LanceConversionWriter *writer);

#ifdef __cplusplus
}
#endif
