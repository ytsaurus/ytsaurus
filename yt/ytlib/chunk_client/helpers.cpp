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

TFuture<TMasterYPathProxy::TRspCreateObjectPtr> CreateChunk(
    NRpc::IChannelPtr masterChannel,
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    EObjectType chunkType,
    TTransactionId transactionId)
{
    auto uploadReplicationFactor = std::min(options->ReplicationFactor, config->UploadReplicationFactor);
    LOG_DEBUG("Creating chunk (ReplicationFactor: %v, UploadReplicationFactor: %v)",
        options->ReplicationFactor,
        uploadReplicationFactor);

    TObjectServiceProxy objectProxy(masterChannel);

    auto req = TMasterYPathProxy::CreateObject();
    ToProto(req->mutable_transaction_id(), transactionId);
    GenerateMutationId(req);
    req->set_type(static_cast<int>(chunkType));
    req->set_account(options->Account);

    auto* reqExt = req->mutable_extensions()->MutableExtension(NProto::TChunkCreationExt::chunk_creation_ext);
    reqExt->set_replication_factor(options->ReplicationFactor);
    reqExt->set_movable(config->ChunksMovable);
    reqExt->set_vital(options->ChunksVital);
    reqExt->set_erasure_codec(static_cast<int>(options->ErasureCodec));

    return objectProxy.Execute(req);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
