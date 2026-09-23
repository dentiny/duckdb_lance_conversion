#pragma once

#include "duckdb/common/exception.hpp"
#include "lance_conversion.h"

#include <memory>

namespace duckdb {

struct LanceErrorDeleter {
	void operator()(char *error) const {
		lance_conversion_error_free(error);
	}
};

inline void ThrowIfLanceError(char *error, const char *context) {
	std::unique_ptr<char, LanceErrorDeleter> owned(error);
	if (owned) {
		throw IOException("%s: %s", context, owned.get());
	}
}

} // namespace duckdb
