#include "public.h"
#include "chunk_owner_ypath_proxy.h"

#include <ytlib/api/public.h>

#include <ytlib/object_client/master_ypath_proxy.h>

#include <ytlib/transaction_client/public.h>

#include <ytlib/node_tracker_client/public.h>

#include <core/actions/public.h>

#include <core/erasure/public.h>

#include <core/rpc/public.h>

#include <core/logging/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TChunkId CreateChunk(
    NApi::IClientPtr client,
    NObjectClient::TCellTag cellTag,
    TMultiChunkWriterOptionsPtr options,
    const NObjectClient::TTransactionId& transactionId,
    const TChunkListId& chunkListId,
    const NLogging::TLogger& logger);

//! Synchronously parses #fetchResponse, populates #nodeDirectory,
//! issues additional |LocateChunks| requests for foreign chunks.
//! The resulting chunk specs are appended to #chunkSpecs.
void ProcessFetchResponse(
    NApi::IClientPtr client,
    TChunkOwnerYPathProxy::TRspFetchPtr fetchResponse,
    NObjectClient::TCellTag fetchCellTag,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    int maxChunksPerLocateRequest,
    const NLogging::TLogger& logger,
    std::vector<NProto::TChunkSpec>* chunkSpecs);

////////////////////////////////////////////////////////////////////////////////

IChunkReaderPtr CreateRemoteReader(
    const TChunkId& chunkId,
    const TChunkReplicaList& replicas,
    NErasure::ECodec erasureCodecId,
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IBlockCachePtr blockCache,
    NConcurrency::IThroughputThrottlerPtr throttler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
