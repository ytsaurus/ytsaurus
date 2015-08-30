#include "helpers.h"
#include "config.h"

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>

#include <ytlib/api/client.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NChunkClient {

using namespace NApi;
using namespace NRpc;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NConcurrency;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

NChunkClient::TChunkId CreateChunk(
    IClientPtr client,
    TCellTag cellTag,
    TMultiChunkWriterConfigPtr config,
    TMultiChunkWriterOptionsPtr options,
    EObjectType chunkType,
    const TTransactionId& transactionId,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    auto uploadReplicationFactor = std::min(options->ReplicationFactor, config->UploadReplicationFactor);
    LOG_DEBUG("Creating chunk (ReplicationFactor: %v, UploadReplicationFactor: %v)",
        options->ReplicationFactor,
        uploadReplicationFactor);

    auto channel = client->GetMasterChannel(EMasterChannelKind::Leader, cellTag);
    TObjectServiceProxy proxy(channel);

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

    auto rspOrError = WaitFor(proxy.Execute(req));
    THROW_ERROR_EXCEPTION_IF_FAILED(
        rspOrError,
        NChunkClient::EErrorCode::ChunkCreationFailed,
        "Error creating chunk");

    const auto& rsp = rspOrError.Value();
    return FromProto<TChunkId>(rsp->object_id());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
