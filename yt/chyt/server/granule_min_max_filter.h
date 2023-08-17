#pragma once

#include "private.h"

#include <yt/yt/ytlib/table_client/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

NTableClient::IGranuleFilterPtr CreateGranuleMinMaxFilter(
    const DB::SelectQueryInfo& queryInfo,
    TCompositeSettingsPtr settings,
    const NTableClient::TTableSchemaPtr& schema,
    const DB::ContextPtr& context,
    const std::vector<TString>& realColumnNames);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
