#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>
#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/node_tracker_client/public.h>
#include <yt/ytlib/table_client/public.h>

#include <yt/core/concurrency/public.h>

#include <vector>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

NTableClient::ISchemafulReaderPtr CreateChunkReader(
    NTableClient::TTableReaderConfigPtr config,
    NTableClient::TTableReaderOptionsPtr options,
    NApi::NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    NChunkClient::TDataSourceDirectoryPtr dataSourceDirectory,
    const std::vector<NChunkClient::TDataSliceDescriptor>& dataSliceDescriptors,
    NConcurrency::IThroughputThrottlerPtr bandwidthThrottler,
    const NTableClient::TTableSchema& readerSchema,
    bool allowUnorderedRead);

void WarmUp(std::vector<NTableClient::ISchemafulReaderPtr>& chunkReaders);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
