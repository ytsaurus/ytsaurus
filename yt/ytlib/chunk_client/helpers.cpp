#include "helpers.h"
#include "config.h"
#include "chunk_service_proxy.h"

#include <ytlib/object_client/object_service_proxy.h>
#include <ytlib/object_client/master_ypath_proxy.h>
#include <ytlib/object_client/helpers.h>

#include <ytlib/chunk_client/chunk_ypath_proxy.h>

#include <ytlib/api/client.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <core/concurrency/scheduler.h>

namespace NYT {
namespace NChunkClient {

using namespace NApi;
using namespace NRpc;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NNodeTrackerClient;
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
    reqExt->set_movable(options->ChunksMovable);
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

void ProcessFetchResponse(
    IClientPtr client,
    TChunkOwnerYPathProxy::TRspFetchPtr fetchResponse,
    TCellTag fetchCellTag,
    TNodeDirectoryPtr nodeDirectory,
    int maxChunksPerLocateRequest,
    const NLogging::TLogger& logger,
    std::vector<NProto::TChunkSpec>* chunkSpecs)
{
    const auto& Logger = logger;

    nodeDirectory->MergeFrom(fetchResponse->node_directory());

    yhash_map<TCellTag, std::vector<NProto::TChunkSpec*>> foreignChunkMap;
    for (auto& chunkSpec : *fetchResponse->mutable_chunks()) {
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto chunkCellTag = CellTagFromId(chunkId);
        if (chunkCellTag != fetchCellTag) {
            foreignChunkMap[chunkCellTag].push_back(&chunkSpec);
        }
    }

    for (const auto& pair : foreignChunkMap) {
        auto foreignCellTag = pair.first;
        auto& foreignChunkSpecs = pair.second;

        auto channel = client->GetMasterChannel(EMasterChannelKind::LeaderOrFollower, foreignCellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < foreignChunkSpecs.size(); beginIndex += maxChunksPerLocateRequest) {
            int endIndex = std::min(
                beginIndex + maxChunksPerLocateRequest,
                static_cast<int>(foreignChunkSpecs.size()));

            auto req = proxy.LocateChunks();
            for (int index = beginIndex; index < endIndex; ++index) {
                req->add_chunk_ids()->CopyFrom(foreignChunkSpecs[index]->chunk_id());
            }

            LOG_INFO("Locating foreign chunks (CellTag: %v, ChunkCount: %v)",
                foreignCellTag,
                req->chunk_ids_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error locating foreign chunks at cell %v",
                foreignCellTag);
            const auto& rsp = rspOrError.Value();

            nodeDirectory->MergeFrom(rsp->node_directory());

            for (int index = beginIndex; index < endIndex; ++index) {
                int rspIndex = index - beginIndex;
                auto expectedChunkId = FromProto<TChunkId>(foreignChunkSpecs[index]->chunk_id());
                auto actualChunkId = rspIndex < rsp->chunks_size()
                    ? FromProto<TChunkId>(rsp->chunks(rspIndex).chunk_id())
                    : NullChunkId;
                if (expectedChunkId != actualChunkId) {
                    THROW_ERROR_EXCEPTION(
                        NChunkClient::EErrorCode::NoSuchChunk,
                        "No such chunk %v",
                        expectedChunkId);
                }
                foreignChunkSpecs[index]->mutable_replicas()->Swap(rsp->mutable_chunks(rspIndex)->mutable_replicas());
            }
        }
    }

    for (auto& chunkSpec : *fetchResponse->mutable_chunks()) {
        chunkSpecs->push_back(NProto::TChunkSpec());
        chunkSpecs->back().Swap(&chunkSpec);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
