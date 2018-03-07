#pragma once

#include "public.h"
#include "schemaless_chunk_reader.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

ISchemalessMultiChunkReaderPtr CreateSchemalessPartitionSortReader(
    NChunkClient::TMultiChunkReaderConfigPtr config,
    NApi::INativeClientPtr client,
    NChunkClient::IBlockCachePtr blockCache,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TKeyColumns& keyColumns,
    TNameTablePtr nameTable,
    TClosure onNetworkReleased,
    const NChunkClient::TDataSourceDirectoryPtr& dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    i64 estimatedRowCount,
    bool isApproximate,
    int partitionTag,
    NChunkClient::TTrafficMeterPtr trafficMeter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
