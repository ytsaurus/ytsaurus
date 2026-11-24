#pragma once

#include <src/client/impl/internal/internal_header.h>

#include <string>

namespace NYdb::inline V3 {

struct TConnectionInfo {
    std::string Endpoint = "";
    std::string Database = "";
    bool EnableSsl = false;
};

TConnectionInfo ParseConnectionString(const std::string& connectionString);

} // namespace NYdb
