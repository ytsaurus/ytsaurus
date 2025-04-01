#pragma once

#include "private.h"
#include "query_progress.h"

#include <yt/yt/core/misc/statistics.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

//! Stores information about the finished query that may be used after the query
//! context is destroyed, e.g. to enrich system.query_log during the export.
struct TQueryFinishInfo
{
    TStatistics Statistics;
    TQueryProgressValues Progress;
    NYTree::IAttributeDictionaryPtr RuntimeVariables;
    std::vector<TQueryId> SecondaryQueryIds;
    std::vector<TQueryId> AdditionalQueryIds;
    std::vector<std::pair<TString, TString>> HttpHeaders;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
