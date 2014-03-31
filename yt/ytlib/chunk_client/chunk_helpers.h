#include "public.h"

#include <core/actions/public.h>
#include <core/rpc/public.h>

#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TFuture<NObjectClient::TMasterYPathProxy::TRspCreateObjectsPtr> CreateChunk(
    NRpc::IChannelPtr masterChannel,
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    NObjectClient::EObjectType chunkType,
    NObjectClient::TTransactionId transactionId);

void OnChunkCreated(
    NObjectClient::TMasterYPathProxy::TRspCreateObjectsPtr rsp,
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    TChunkId* chunkId,
    std::vector<TChunkReplica>* replicas,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
