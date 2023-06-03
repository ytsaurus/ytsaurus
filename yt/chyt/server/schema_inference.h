#include "private.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Returns the common schema for all tables. All tables can be read safely with returned schema via native readers.
//! In case of missing column or column type mismatch, the behavior is defined according to the settings.
//! Key columns are the maximum common prefix of key columns in all tables.
NTableClient::TTableSchemaPtr InferCommonTableSchema(
    const std::vector<TTablePtr>& tables,
    const TConcatTablesSettingsPtr& settings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
