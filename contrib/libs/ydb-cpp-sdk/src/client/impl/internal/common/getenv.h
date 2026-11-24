#pragma once

#include <src/client/impl/internal/internal_header.h>

#include <string>

namespace NYdb::inline V3 {

std::string GetStrFromEnv(const char* envVarName, const std::string& defaultValue = "");

} // namespace NYdb
