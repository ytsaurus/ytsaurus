#pragma once

#include "cluster_nodes.h"
#include "private.h"

#include <QueryPipeline/Pipe.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::Pipe CreateRemoteSource(
    const IClusterNodePtr& remoteNode,
    const TSecondaryQuery& secondaryQuery,
    TQueryId remoteQueryId,
    DB::ContextPtr context,
    const DB::ThrottlerPtr& throttler,
    const DB::Tables& externalTables,
    DB::QueryProcessingStage::Enum processingStage,
    const DB::Block& blockHeader,
    NLogging::TLogger logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
