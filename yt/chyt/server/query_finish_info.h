#pragma once

#include "private.h"

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Stores information about the finished query that may be used after the query
//! context is destroyed, e.g. to enrich system.query_log during the export.
struct TQueryFinishInfo
{
    TStatistics Statistics;
    std::vector<TQueryId> SecondaryQueryIds;;
};

////////////////////////////////////////////////////////////////////////////////

} // NYT::NClickHouseServer
