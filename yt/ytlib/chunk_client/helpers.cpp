#include "helpers.h"
#include "private.h"
#include "config.h"
#include "input_chunk_slice.h"
#include "replication_reader.h"
#include "repairing_reader.h"

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/client/chunk_client/chunk_replica.h>
#include <yt/client/chunk_client/data_statistics.h>

#include <yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/cypress_client/rpc_helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/ytlib/job_tracker_client/statistics.h>

#include <yt/client/object_client/helpers.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/compression/codec.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/net/local_address.h>

#include <array>

namespace NYT::NChunkClient {

using namespace NApi;
using namespace NRpc;
using namespace NConcurrency;
using namespace NLogging;
using namespace NObjectClient;
using namespace NErasure;
using namespace NNodeTrackerClient;
using namespace NYPath;
using namespace NYTree;
using namespace NNet;
using namespace NCypressClient;

using NYT::FromProto;
using NYT::ToProto;
using NNodeTrackerClient::TNodeId;

////////////////////////////////////////////////////////////////////////////////

TSessionId CreateChunk(
    NNative::IClientPtr client,
    TCellTag cellTag,
    TMultiChunkWriterOptionsPtr options,
    TTransactionId transactionId,
    const TChunkListId& chunkListId,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    YT_LOG_DEBUG("Creating chunk (ReplicationFactor: %v, TransactionId: %v, ChunkListId: %v, MediumName: %v)",
        options->ReplicationFactor,
        transactionId,
        chunkListId,
        options->MediumName);

    auto chunkType = options->ErasureCodec == ECodec::None
        ? EObjectType::Chunk
        : EObjectType::ErasureChunk;

    auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, cellTag);
    TChunkServiceProxy proxy(channel);

    auto batchReq = proxy.ExecuteBatch();
    GenerateMutationId(batchReq);
    batchReq->set_suppress_upstream_sync(true);

    auto* req = batchReq->add_create_chunk_subrequests();
    ToProto(req->mutable_transaction_id(), transactionId);
    req->set_type(static_cast<int>(chunkType));
    req->set_account(options->Account);
    req->set_replication_factor(options->ReplicationFactor);
    req->set_movable(options->ChunksMovable);
    req->set_vital(options->ChunksVital);
    req->set_erasure_codec(static_cast<int>(options->ErasureCodec));
    req->set_medium_name(options->MediumName);
    req->set_validate_resource_usage_increase(options->ValidateResourceUsageIncrease);
    if (chunkListId) {
        ToProto(req->mutable_chunk_list_id(), chunkListId);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        GetCumulativeError(batchRspOrError),
        NChunkClient::EErrorCode::MasterCommunicationFailed,
        "Error creating chunk");

    const auto& batchRsp = batchRspOrError.Value();
    const auto& rsp = batchRsp->create_chunk_subresponses(0);
    auto sessionId = FromProto<TSessionId>(rsp.session_id());

    YT_LOG_DEBUG("Chunk created (MediumIndex: %v)",
        sessionId.MediumIndex);

    return sessionId;
}

void ProcessFetchResponse(
    NNative::IClientPtr client,
    TChunkOwnerYPathProxy::TRspFetchPtr fetchResponse,
    TCellTag fetchCellTag,
    TNodeDirectoryPtr nodeDirectory,
    int maxChunksPerLocateRequest,
    std::optional<int> rangeIndex,
    const NLogging::TLogger& logger,
    std::vector<NProto::TChunkSpec>* chunkSpecs,
    bool skipUnavialableChunks)
{
    if (nodeDirectory) {
        nodeDirectory->MergeFrom(fetchResponse->node_directory());
    }

    std::vector<NProto::TChunkSpec*> foreignChunkList;
    for (auto& chunkSpec : *fetchResponse->mutable_chunks()) {
        if (rangeIndex) {
            chunkSpec.set_range_index(*rangeIndex);
        }
        auto chunkId = FromProto<TChunkId>(chunkSpec.chunk_id());
        auto chunkCellTag = CellTagFromId(chunkId);
        if (chunkCellTag != fetchCellTag) {
            foreignChunkList.push_back(&chunkSpec);
        }
    }
    LocateChunks(client, maxChunksPerLocateRequest, foreignChunkList, nodeDirectory, logger, skipUnavialableChunks);

    for (auto& chunkSpec : *fetchResponse->mutable_chunks()) {
        chunkSpecs->push_back(NProto::TChunkSpec());
        chunkSpecs->back().Swap(&chunkSpec);
    }
}

void FetchChunkSpecs(
    const NNative::IClientPtr& client,
    const TNodeDirectoryPtr& nodeDirectory,
    TCellTag cellTag,
    const TYPath& path,
    const std::vector<NChunkClient::TReadRange>& ranges,
    int chunkCount,
    int maxChunksPerFetch,
    int maxChunksPerLocateRequest,
    const std::function<void(TChunkOwnerYPathProxy::TReqFetchPtr)> initializeFetchRequest,
    const NLogging::TLogger& logger,
    std::vector<NProto::TChunkSpec>* chunkSpecs,
    bool skipUnavailableChunks)
{
    std::vector<int> rangeIndices;

    auto channel = client->GetMasterChannelOrThrow(
        EMasterChannelKind::Follower,
        cellTag);
    TObjectServiceProxy proxy(channel);
    auto batchReq = proxy.ExecuteBatch();

    for (int rangeIndex = 0; rangeIndex < static_cast<int>(ranges.size()); ++rangeIndex) {
        for (i64 index = 0; index < (chunkCount + maxChunksPerFetch - 1) / maxChunksPerFetch; ++index) {
            auto adjustedRange = ranges[rangeIndex];
            auto chunkCountLowerLimit = index * maxChunksPerFetch;
            if (adjustedRange.LowerLimit().HasChunkIndex()) {
                chunkCountLowerLimit = std::max(chunkCountLowerLimit, adjustedRange.LowerLimit().GetChunkIndex());
            }
            adjustedRange.LowerLimit().SetChunkIndex(chunkCountLowerLimit);

            auto chunkCountUpperLimit = (index + 1) * maxChunksPerFetch;
            if (adjustedRange.UpperLimit().HasChunkIndex()) {
                chunkCountUpperLimit = std::min(chunkCountUpperLimit, adjustedRange.UpperLimit().GetChunkIndex());
            }
            adjustedRange.UpperLimit().SetChunkIndex(chunkCountUpperLimit);

            auto req = TChunkOwnerYPathProxy::Fetch(path);
            initializeFetchRequest(req.Get());
            NYT::ToProto(req->mutable_ranges(), std::vector<NChunkClient::TReadRange>{adjustedRange});
            batchReq->AddRequest(req, "fetch");
            rangeIndices.push_back(rangeIndex);
        }
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(GetCumulativeError(batchRspOrError), "Error fetching input table %v", path);
    const auto& batchRsp = batchRspOrError.Value();
    auto rspsOrError = batchRsp->GetResponses<TChunkOwnerYPathProxy::TRspFetch>("fetch");

    for (int resultIndex = 0; resultIndex < static_cast<int>(rspsOrError.size()); ++resultIndex) {
        const auto& rsp = rspsOrError[resultIndex].Value();
        ProcessFetchResponse(
            client,
            rsp,
            cellTag,
            nodeDirectory,
            maxChunksPerLocateRequest,
            rangeIndices[resultIndex],
            logger,
            chunkSpecs,
            skipUnavailableChunks);
    }
}

TChunkReplicaList AllocateWriteTargets(
    NNative::IClientPtr client,
    const TSessionId& sessionId,
    int desiredTargetCount,
    int minTargetCount,
    std::optional<int> replicationFactorOverride,
    bool preferLocalHost,
    const std::vector<TString>& forbiddenAddresses,
    TNodeDirectoryPtr nodeDirectory,
    const NLogging::TLogger& logger)
{
    const auto& Logger = logger;

    YT_LOG_DEBUG("Allocating write targets "
        "(ChunkId: %v, DesiredTargetCount: %v, MinTargetCount: %v, PreferLocalHost: %v, "
        "ForbiddenAddresses: %v)",
        sessionId,
        desiredTargetCount,
        minTargetCount,
        preferLocalHost,
        forbiddenAddresses);

    auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Leader, CellTagFromId(sessionId.ChunkId));
    TChunkServiceProxy proxy(channel);

    auto batchReq = proxy.AllocateWriteTargets();
    auto* req = batchReq->add_subrequests();
    req->set_desired_target_count(desiredTargetCount);
    req->set_min_target_count(minTargetCount);
    if (replicationFactorOverride) {
        req->set_replication_factor_override(*replicationFactorOverride);
    }
    if (preferLocalHost) {
        req->set_preferred_host_name(GetLocalHostName());
    }
    ToProto(req->mutable_forbidden_addresses(), forbiddenAddresses);
    ToProto(req->mutable_session_id(), sessionId);

    auto batchRspOrError = WaitFor(batchReq->Invoke());

    auto throwOnError = [&] (const TError& error) {
        THROW_ERROR_EXCEPTION_IF_FAILED(
            error,
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Error allocating targets for chunk %v",
            sessionId);
    };

    throwOnError(batchRspOrError);
    const auto& batchRsp = batchRspOrError.Value();

    nodeDirectory->MergeFrom(batchRsp->node_directory());

    auto& rsp = batchRsp->subresponses(0);
    if (rsp.has_error()) {
        throwOnError(FromProto<TError>(rsp.error()));
    }

    auto replicas = FromProto<TChunkReplicaList>(rsp.replicas());
    if (replicas.empty()) {
        THROW_ERROR_EXCEPTION(
            NChunkClient::EErrorCode::MasterCommunicationFailed,
            "Not enough data nodes available to write chunk %v",
            sessionId);
    }

    YT_LOG_DEBUG("Write targets allocated (ChunkId: %v, Targets: %v)",
        sessionId,
        MakeFormattableRange(replicas, TChunkReplicaAddressFormatter(nodeDirectory)));

    return replicas;
}

TError GetCumulativeError(const TChunkServiceProxy::TErrorOrRspExecuteBatchPtr& batchRspOrError)
{
    if (!batchRspOrError.IsOK()) {
        return batchRspOrError;
    }

    const auto& batchRsp = batchRspOrError.Value();
    TError cumulativeError("Error executing chunk operations");

    auto processSubresponses = [&] (const auto& subresponses) {
        for (const auto& subresponse : subresponses) {
            if (subresponse.has_error()) {
                cumulativeError.InnerErrors().push_back(FromProto<TError>(subresponse.error()));
            }
        }
    };
    processSubresponses(batchRsp->create_chunk_subresponses());
    processSubresponses(batchRsp->confirm_chunk_subresponses());
    processSubresponses(batchRsp->seal_chunk_subresponses());
    processSubresponses(batchRsp->create_chunk_lists_subresponses());
    processSubresponses(batchRsp->unstage_chunk_tree_subresponses());
    processSubresponses(batchRsp->attach_chunk_trees_subresponses());

    return cumulativeError.InnerErrors().empty() ? TError() : cumulativeError;
}

////////////////////////////////////////////////////////////////////////////////

i64 GetChunkDataWeight(const NProto::TChunkSpec& chunkSpec)
{
    if (chunkSpec.has_data_weight_override()) {
        return chunkSpec.data_weight_override();
    }
    const auto& miscExt = GetProtoExtension<NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
    return miscExt.data_weight();
}

i64 GetChunkUncompressedDataSize(const NProto::TChunkSpec& chunkSpec)
{
    const auto& miscExt = GetProtoExtension<NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
    return miscExt.uncompressed_data_size();
}

i64 GetChunkReaderMemoryEstimate(const NProto::TChunkSpec& chunkSpec, TMultiChunkReaderConfigPtr config)
{
    // Misc may be cleared out by the scheduler (e.g. for partition chunks).
    auto miscExt = FindProtoExtension<NProto::TMiscExt>(chunkSpec.chunk_meta().extensions());
    if (miscExt) {
        // NB: data weight is upper bound on the uncompressed data size.
        i64 currentSize = GetChunkDataWeight(chunkSpec);

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
    const NProto::TChunkSpec& chunkSpec,
    TErasureReaderConfigPtr config,
    TRemoteReaderOptionsPtr options,
    NNative::IClientPtr client,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    const TNodeDescriptor& localDescriptor,
    IBlockCachePtr blockCache,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr bandwidthThrottler,
    IThroughputThrottlerPtr rpsThrottler)
{
    auto chunkId = NYT::FromProto<TChunkId>(chunkSpec.chunk_id());
    auto replicas = NYT::FromProto<TChunkReplicaList>(chunkSpec.replicas());

    auto Logger = TLogger(ChunkClientLogger).AddTag("ChunkId: %v", chunkId);

    if (IsErasureChunkId(chunkId)) {
        auto erasureCodecId = ECodec(chunkSpec.erasure_codec());
        YT_LOG_DEBUG("Creating erasure remote reader (Codec: %v)", erasureCodecId);

        std::array<TNodeId, MaxTotalPartCount> partIndexToNodeId;
        std::fill(partIndexToNodeId.begin(), partIndexToNodeId.end(), InvalidNodeId);
        std::array<int, MaxTotalPartCount> partIndexToMediumIndex;
        std::fill(partIndexToMediumIndex.begin(), partIndexToMediumIndex.end(), DefaultStoreMediumIndex);
        for (auto replica : replicas) {
            auto replicaIndex = replica.GetReplicaIndex();
            partIndexToNodeId[replicaIndex] = replica.GetNodeId();
            partIndexToMediumIndex[replicaIndex] = replica.GetMediumIndex();
        }

        auto* erasureCodec = GetCodec(erasureCodecId);
        auto partCount = config->EnableAutoRepair ?
            erasureCodec->GetTotalPartCount() :
            erasureCodec->GetDataPartCount();

        std::vector<IChunkReaderAllowingRepairPtr> readers;
        readers.reserve(partCount);

        for (int index = 0; index < partCount; ++index) {
            TChunkReplicaList partReplicas;
            auto nodeId = partIndexToNodeId[index];
            auto mediumIndex = partIndexToMediumIndex[index];
            if (nodeId != InvalidNodeId) {
                partReplicas.push_back(TChunkReplica(nodeId, index, mediumIndex));
            }

            auto partId = ErasurePartIdFromChunkId(chunkId, index);

            auto reader = CreateReplicationReader(
                config,
                options,
                client,
                nodeDirectory,
                localDescriptor,
                partId,
                partReplicas,
                blockCache,
                trafficMeter,
                bandwidthThrottler,
                rpsThrottler);
            readers.push_back(reader);
        }

        return CreateRepairingReader(erasureCodec, config, readers, Logger);
    } else {
        YT_LOG_DEBUG("Creating regular remote reader");

        return CreateReplicationReader(
            config,
            options,
            client,
            nodeDirectory,
            localDescriptor,
            chunkId,
            replicas,
            blockCache,
            trafficMeter,
            bandwidthThrottler,
            rpsThrottler);
    }
}

void LocateChunks(
    NApi::NNative::IClientPtr client,
    int maxChunksPerLocateRequest,
    const std::vector<NProto::TChunkSpec*> chunkSpecList,
    const NNodeTrackerClient::TNodeDirectoryPtr& nodeDirectory,
    const NLogging::TLogger& logger,
    bool skipUnavailableChunks)
{
    const auto& Logger = logger;

    THashMap<NObjectClient::TCellTag, std::vector<NProto::TChunkSpec*>> chunkMap;

    for (auto* chunkSpec : chunkSpecList) {
        auto chunkId = FromProto<TChunkId>(chunkSpec->chunk_id());
        auto chunkCellTag = CellTagFromId(chunkId);
        auto& cellChunkList = chunkMap[chunkCellTag];
        cellChunkList.push_back(chunkSpec);
    }

    for (auto& pair : chunkMap) {
        auto cellTag = pair.first;
        auto& chunkSpecs = pair.second;

        auto channel = client->GetMasterChannelOrThrow(EMasterChannelKind::Follower, cellTag);
        TChunkServiceProxy proxy(channel);

        for (int beginIndex = 0; beginIndex < chunkSpecs.size(); beginIndex += maxChunksPerLocateRequest) {
            int endIndex = std::min(
                beginIndex + maxChunksPerLocateRequest,
                static_cast<int>(chunkSpecs.size()));

            auto req = proxy.LocateChunks();
            req->SetHeavy(true);
            for (int index = beginIndex; index < endIndex; ++index) {
                *req->add_subrequests() = chunkSpecs[index]->chunk_id();
            }

            YT_LOG_DEBUG("Locating chunks (CellTag: %v, ChunkCount: %v)",
                cellTag,
                req->subrequests_size());

            auto rspOrError = WaitFor(req->Invoke());
            THROW_ERROR_EXCEPTION_IF_FAILED(rspOrError, "Error locating chunks at cell %v",
                cellTag);
            const auto& rsp = rspOrError.Value();
            YCHECK(req->subrequests_size() == rsp->subresponses_size());

            if (nodeDirectory) {
                nodeDirectory->MergeFrom(rsp->node_directory());
            }

            for (int globalIndex = beginIndex; globalIndex < endIndex; ++globalIndex) {
                int localIndex = globalIndex - beginIndex;
                const auto& subrequest = req->subrequests(localIndex);
                auto* subresponse = rsp->mutable_subresponses(localIndex);
                auto chunkId = FromProto<TChunkId>(subrequest);
                if (subresponse->missing()) {
                    if (!skipUnavailableChunks) {
                        THROW_ERROR_EXCEPTION(
                            NChunkClient::EErrorCode::NoSuchChunk,
                            "No such chunk %v",
                            chunkId);
                    } else {
                        chunkSpecs[globalIndex]->mutable_replicas();
                    }
                } else {
                    chunkSpecs[globalIndex]->mutable_replicas()->Swap(subresponse->mutable_replicas());
                    chunkSpecs[globalIndex]->set_erasure_codec(subresponse->erasure_codec());
                }
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TString TUserObject::GetPath() const
{
    return Path.GetPath();
}

bool TUserObject::IsPrepared() const
{
    return static_cast<bool>(ObjectId);
}

void TUserObject::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, Path);
    Persist(context, ObjectId);
    Persist(context, CellTag);
}

////////////////////////////////////////////////////////////////////////////////

i64 CalculateDiskSpaceUsage(
    int replicationFactor,
    i64 regularDiskSpace,
    i64 erasureDiskSpace)
{
    // NB: replicationFactor == 0 for unused media.
    return replicationFactor > 0
        ? regularDiskSpace * replicationFactor + erasureDiskSpace
        : 0;
}

////////////////////////////////////////////////////////////////////////////////

void DumpCodecStatistics(
    const TCodecStatistics& codecStatistics,
    const NYPath::TYPath& path,
    NJobTrackerClient::TStatistics* statistics)
{
    for (const auto& pair : codecStatistics.CodecToDuration()) {
        statistics->AddSample(path + '/' + FormatEnum(pair.first), pair.second);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
