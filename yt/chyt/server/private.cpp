#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const std::string CacheUserName("yt-clickhouse-cache");
const std::string ChytSqlObjectsUserName("chyt-sql-objects");
const std::string DictionariesUserName("yt-clikhouse-dictionaries");
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

const TString LowCardinalityTag("$low_cardinality");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
