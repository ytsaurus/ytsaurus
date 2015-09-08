#include "helpers.h"
#include "config.h"
#include "private.h"

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFuture<TMasterYPathProxy::TRspCreateObjectsPtr> CreateChunk(
    NRpc::IChannelPtr masterChannel,
    TMultiChunkWriterOptionsPtr options,
    EObjectType chunkType,
    const TTransactionId& transactionId,
    const TChunkListId& chunkListId)
{
    LOG_DEBUG(
        "Creating chunk (ReplicationFactor: %v, TransactionId: %v)", 
        options->ReplicationFactor, 
        transactionId);

    TObjectServiceProxy objectProxy(masterChannel);

    auto req = TMasterYPathProxy::CreateObjects();
    ToProto(req->mutable_transaction_id(), transactionId);
    GenerateMutationId(req);
    req->set_type(static_cast<int>(chunkType));
    req->set_account(options->Account);

    auto* reqExt = req->MutableExtension(NProto::TReqCreateChunkExt::create_chunk_ext);
    reqExt->set_replication_factor(options->ReplicationFactor);
    reqExt->set_movable(options->ChunksMovable);
    reqExt->set_vital(options->ChunksVital);
    reqExt->set_erasure_codec(static_cast<int>(options->ErasureCodec));
    if (chunkListId != NullChunkListId) {
        ToProto(reqExt->mutable_chunk_list_id(), chunkListId);
    }

    return objectProxy.Execute(req);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
