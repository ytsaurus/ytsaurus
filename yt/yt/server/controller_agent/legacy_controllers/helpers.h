#pragma once

#include "private.h"

#include "data_flow_graph.h"

#include <yt/server/lib/legacy_chunk_pools/chunk_stripe_key.h>

#include <yt/ytlib/chunk_client/helpers.h>

#include <yt/ytlib/table_client/helpers.h>

namespace NYT::NControllerAgent::NLegacyControllers {

////////////////////////////////////////////////////////////////////////////////

NLegacyChunkPools::TBoundaryKeys BuildBoundaryKeysFromOutputResult(
    const NScheduler::NProto::TOutputResult& boundaryKeys,
    const TEdgeDescriptor& outputTable,
    const NTableClient::TRowBufferPtr& rowBuffer);

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TDataSourceDirectoryPtr BuildDataSourceDirectoryFromInputTables(const std::vector<TInputTablePtr>& inputTables);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NLegacyControllers
