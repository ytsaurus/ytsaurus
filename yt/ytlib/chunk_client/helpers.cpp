#include "helpers.h"
#include "private.h"
#include "config.h"
#include "chunk_slice.h"
#include "erasure_reader.h"
#include "replication_reader.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_replica.h>
#include <yt/ytlib/chunk_client/chunk_ypath_proxy.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>
#include <yt/ytlib/object_client/helpers.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/compression/codec.h>

#include <yt/core/erasure/codec.h>

namespace NYT {
namespace NChunkClient {

using namespace NApi;
using namespace NRpc;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NErasure;
using namespace NNodeTrackerClient;
using namespace NProto;
using namespace NApi;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

TChunkId CreateChunk(
    IClientPtr client,
    TCellTag cellTag,
    TMultiChunkWriterOptionsPtr options,
    const TTransactionId& transactionId,
    const TChunkListId& chunkListId,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    LOG_DEBUG("Creating chunk (ReplicationFactor: %v, TransactionId: %v, ChunkListId: %v)",
        options->ReplicationFactor, 
        transactionId,
        chunkListId);

    auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TObjectServiceProxy proxy(channel);

    auto chunkType = options->ErasureCodec == ECodec::None
         ? EObjectType::Chunk
         : EObjectType::ErasureChunk;

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
    if (chunkListId) {
        ToProto(reqExt->mutable_chunk_list_id(), chunkListId);
    }

    auto rspOrError = WaitFor(proxy.Execute(req));
    const auto& rsp = rspOrError.ValueOrThrow();
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

        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::LeaderOrFollower, foreignCellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < foreignChunkSpecs.size(); beginIndex += maxChunksPerLocateRequest) {
            int endIndex = std::min(
                beginIndex + maxChunksPerLocateRequest,
                static_cast<int>(foreignChunkSpecs.size()));

            auto req = proxy.LocateChunks();
            for (int index = beginIndex; index < endIndex; ++index) {
                req->add_chunk_ids()->CopyFrom(foreignChunkSpecs[index]->chunk_id());
            }

            LOG_DEBUG("Locating foreign chunks (CellTag: %v, ChunkCount: %v)",
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

i64 GetChunkReaderMemoryEstimate(const TChunkSpec& chunkSpec, TMultiChunkReaderConfigPtr config)
{
    // Misc may be cleared out by the scheduler (e.g. for partition chunks).
    auto miscExt = FindProtoExtension<TMiscExt>(chunkSpec.chunk_meta().extensions());
    if (miscExt) {
        i64 currentSize;
        GetStatistics(chunkSpec, &currentSize);

        // Block used by upper level chunk reader.
        i64 chunkBufferSize = ChunkReaderMemorySize + miscExt->max_block_size();

        if (currentSize > miscExt->max_block_size()) {
            chunkBufferSize += config->WindowSize + config->GroupSize;
        }
        return chunkBufferSize;
    } else {
        return ChunkReaderMemorySize + 
            config->WindowSize + 
            config->GroupSize + 
            DefaultMaxBlockSize;
    }
}

IChunkReaderPtr CreateRemoteReader(
    const TChunkSpec& chunkSpec,
    TReplicationReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NApi::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TNullable<TNodeDescriptor>& localDescriptor,
    IBlockCachePtr blockCache,
    IThroughputThrottlerPtr throttler)
{
    auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
    auto replicas = FromProto<TChunkReplicaList>(chunkSpec.replicas());

    LOG_DEBUG("Creating remote reader (ChunkId: %v)", chunkId);

    if (IsErasureChunkId(chunkId)) {
        std::sort(
            replicas.begin(),
            replicas.end(),
            [] (TChunkReplica lhs, TChunkReplica rhs) {
                return lhs.GetIndex() < rhs.GetIndex();
            });

        auto erasureCodecId = ECodec(chunkSpec.erasure_codec());
        auto* erasureCodec = GetCodec(erasureCodecId);
        auto dataPartCount = erasureCodec->GetDataPartCount();

        std::vector<IChunkReaderPtr> readers;
        readers.reserve(dataPartCount);

        auto it = replicas.begin();
        while (it != replicas.end() && it->GetIndex() < dataPartCount) {
            auto jt = it;
            while (jt != replicas.end() && it->GetIndex() == jt->GetIndex()) {
                ++jt;
            }

            TChunkReplicaList partReplicas(it, jt);
            auto partId = ErasurePartIdFromChunkId(chunkId, it->GetIndex());
            auto reader = CreateReplicationReader(
                config,
                options,
                client,
                nodeDirectory,
                localDescriptor,
                partId,
                partReplicas,
                blockCache,
                throttler);
            readers.push_back(reader);

            it = jt;
        }

        YCHECK(readers.size() == dataPartCount);
        return CreateNonRepairingErasureReader(readers);
    } else {
        return CreateReplicationReader(
            config,
            options,
            client,
            nodeDirectory,
            localDescriptor,
            chunkId,
            replicas,
            blockCache,
            throttler);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
