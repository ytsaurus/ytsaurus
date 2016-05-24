#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/object_client/master_ypath_proxy.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/erasure/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<NObjectClient::TMasterYPathProxy::TRspCreateObjectsPtr> CreateChunk(
    NRpc::IChannelPtr masterChannel,
    TMultiChunkWriterOptionsPtr options,
    const NTransactionClient::TTransactionId& transactionId,
    const TChunkListId& chunkListId);

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
