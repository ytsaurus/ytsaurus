#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const std::string CacheUserName("yt-clickhouse-cache");
const std::string ChytSqlObjectsUserName("chyt-sql-objects");
const std::string InternalRemoteUserName("$remote");
const std::vector<std::string> TableAttributesToFetch{
    "id",
    "type",
    "dynamic",
    "chunk_count",
    "external",
    "external_cell_tag",
    "revision",
    "boundary_keys",
    "enable_dynamic_store_read",
    "chyt_banned",
    "row_count",
};
const std::string TableSchemaAttribute("schema");
const std::string TableSchemaIdAttribute("schema_id");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

// This is an override of weak symbol from Common/Allocator.h.
// We do not want ClickHouse allocator to use raw mmaps as ytalloc already
// does that by himself.
__attribute__((__used__)) extern const size_t MMAP_THRESHOLD = static_cast<size_t>(1) << 60;
