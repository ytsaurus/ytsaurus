#include "helpers.h"
#include "config.h"
#include "private.h"

#include <core/misc/address.h>
#include <core/misc/protobuf_helpers.h>

#include <core/rpc/helpers.h>

#include <ytlib/object_client/object_service_proxy.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NChunkClient {

using namespace NRpc;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

TFuture<TMasterYPathProxy::TRspCreateObjectsPtr> CreateChunk(
    NRpc::IChannelPtr masterChannel,
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    EObjectType chunkType,
    TTransactionId transactionId)
{
    auto uploadReplicationFactor = std::min(options->ReplicationFactor, config->UploadReplicationFactor);
    LOG_DEBUG("Creating chunk (ReplicationFactor: %d, UploadReplicationFactor: %d)",
        options->ReplicationFactor,
        uploadReplicationFactor);

    TObjectServiceProxy objectProxy(masterChannel);

    auto req = TMasterYPathProxy::CreateObjects();
    ToProto(req->mutable_transaction_id(), transactionId);
    GenerateMutationId(req);
    req->set_type(chunkType);
    req->set_account(options->Account);

    auto* reqExt = req->MutableExtension(NChunkClient::NProto::TReqCreateChunkExt::create_chunk_ext);
    reqExt->set_replication_factor(options->ReplicationFactor);
    reqExt->set_movable(config->ChunksMovable);
    reqExt->set_vital(options->ChunksVital);
    reqExt->set_erasure_codec(options->ErasureCodec);

    return objectProxy.Execute(req);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
