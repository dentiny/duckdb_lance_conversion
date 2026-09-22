#pragma once

#include <stdint.h>

struct ArrowSchema;
struct ArrowArray;
struct LanceConversionWriter;

#ifdef __cplusplus
extern "C" {
#endif

// Errors are thread-local and valid until the next failing call on that thread.
const char *lance_conversion_last_error(void);
int32_t lance_conversion_open(const char *path, const struct ArrowSchema *schema, int32_t overwrite,
                              struct LanceConversionWriter **output);
// Takes ownership of array on import and clears its release callback.
int32_t lance_conversion_push(struct LanceConversionWriter *writer, struct ArrowArray *array);
int32_t lance_conversion_finish(struct LanceConversionWriter *writer);
// Aborts unfinished writes. Never throws across the ABI.
void lance_conversion_destroy(struct LanceConversionWriter *writer);

#ifdef __cplusplus
}
#endif
