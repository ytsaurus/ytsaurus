#include "distributed_chunk_session_reader.h"

#include "config.h"

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/config.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>
#include <yt/yt/ytlib/chunk_client/replication_reader.h>

#include <yt/yt/ytlib/journal_client/helpers.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/rpc/helpers.h>

#include <yt/yt/core/actions/bind.h>
#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/serialized_invoker.h>

#include <yt/yt/core/misc/backoff_strategy.h>
#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/misc/cast.h>

namespace NYT::NDistributedChunkSessionClient {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NErasure;
using namespace NJournalClient;
using namespace NLogging;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NRpc;

using NApi::NNative::IClientPtr;

using NChunkClient::NProto::TMiscExt;

////////////////////////////////////////////////////////////////////////////////

namespace {

// The reader starts in the active phase and queries replica metadata directly to
// keep up with an unsealed journal chunk. Once writers finish or replica metadata
// reports a seal, it switches to the final phase, learns the final row count if
// needed, and reads the remaining range through the regular replication reader.

DEFINE_ENUM(EPhase,
    (Active)
    (Final)
);

DEFINE_ENUM(EReplicaProgressOutcome,
    (HasData)
    (Sealed)
    (NoNewData)
    (AllNoSuchChunk)
    (AllOtherErrors)
);

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TDistributedChunkSessionReader
    : public IDistributedChunkSessionReader
{
public:
    TDistributedChunkSessionReader(
        TDistributedChunkSessionReaderConfigPtr config,
        IClientPtr client,
        TChunkReaderHostPtr chunkReaderHost,
        TChunkId chunkId,
        TChunkReplicaList replicas,
        int readQuorum,
        i64 startRecordIndex,
        std::optional<i64> rangeEndRecordIndex,
        IInvokerPtr invoker)
        : Config_(std::move(config))
        , Client_(std::move(client))
        , ChunkReaderHost_(std::move(chunkReaderHost))
        , ChunkId_(chunkId)
        , Replicas_(std::move(replicas))
        , ReadQuorum_(readQuorum)
        , RangeEndIndex_(rangeEndRecordIndex)
        , Cursor_(startRecordIndex)
        , Statistics_(New<TDistributedChunkSessionReaderStatistics>())
        , ErrorBackoffStrategy_(Config_->ErrorBackoff)
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Logger(DistributedChunkSessionLogger().WithTag("ChunkId: %v", ChunkId_))
    {
        YT_VERIFY(ReadQuorum_ > 0);
        YT_VERIFY(startRecordIndex >= 0);
        YT_VERIFY(!RangeEndIndex_ || *RangeEndIndex_ >= startRecordIndex);

        YT_LOG_DEBUG(
            "Created distributed chunk session reader (ReadQuorum: %v, StartRecordIndex: %v, "
            "RangeEndRecordIndex: %v, ReplicaCount: %v)",
            ReadQuorum_,
            startRecordIndex,
            RangeEndIndex_,
            Replicas_.size());
    }

    TFuture<TChunkReadResult> Read() override
    {
        return BIND(&TDistributedChunkSessionReader::DoRead, MakeStrong(this))
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    void SetAllWritersFinished(std::optional<i64> finalRecordCount) override
    {
        SerializedInvoker_->Invoke(BIND([this, this_ = MakeStrong(this), finalRecordCount] {
            YT_LOG_DEBUG(
                "All writers finished for distributed chunk session reader "
                "(FinalRecordCount: %v, CurrentPhase: %v, CurrentRecordCount: %v, Cursor: %v)",
                finalRecordCount,
                Phase_,
                ChunkRecordCount_,
                Cursor_);

            Phase_ = EPhase::Final;
            if (finalRecordCount && !ChunkRecordCount_) {
                ChunkRecordCount_ = *finalRecordCount;
            }
        }));
    }

    TDistributedChunkSessionReaderStatisticsConstPtr GetStatistics() const override
    {
        return Statistics_;
    }

private:
    struct TReplicaProgress
    {
        EReplicaProgressOutcome Outcome;
        std::optional<TChunkReplica> PickedReplica;
        i64 PickedRowCount = 0;
        std::vector<TError> InnerErrors;
    };

    const TDistributedChunkSessionReaderConfigPtr Config_;
    const IClientPtr Client_;
    const TChunkReaderHostPtr ChunkReaderHost_;
    const TChunkId ChunkId_;
    TChunkReplicaList Replicas_;
    const int ReadQuorum_;
    const std::optional<i64> RangeEndIndex_;

    EPhase Phase_ = EPhase::Active;
    i64 Cursor_;
    std::optional<i64> ChunkRecordCount_;
    int ErrorAttempts_ = 0;
    std::vector<TError> InnerErrors_;
    bool ReadInProgress_ = false;

    const TDistributedChunkSessionReaderStatisticsPtr Statistics_;
    TBackoffStrategy ErrorBackoffStrategy_;
    const IInvokerPtr SerializedInvoker_;
    const TLogger Logger;

    i64 ComputeEffectiveEnd() const
    {
        i64 end = std::numeric_limits<i64>::max();
        if (RangeEndIndex_) {
            end = std::min(end, *RangeEndIndex_);
        }
        if (ChunkRecordCount_) {
            end = std::min(end, *ChunkRecordCount_);
        }
        return end;
    }

    TChunkReadResult DoRead()
    {
        YT_VERIFY(!ReadInProgress_);
        ReadInProgress_ = true;
        auto finally = Finally([&] {
            ReadInProgress_ = false;
            ErrorAttempts_ = 0;
            InnerErrors_.clear();
            ErrorBackoffStrategy_.Restart();
        });

        YT_LOG_DEBUG(
            "Started distributed chunk session reader read (Phase: %v, Cursor: %v, EffectiveEnd: %v)",
            Phase_,
            Cursor_,
            ComputeEffectiveEnd());

        while (ErrorAttempts_ < Config_->MaxReadAttempts) {
            if (Cursor_ >= ComputeEffectiveEnd()) {
                YT_LOG_DEBUG(
                    "Finished distributed chunk session reader read at effective end "
                    "(Cursor: %v, EffectiveEnd: %v, ErrorAttempts: %v)",
                    Cursor_,
                    ComputeEffectiveEnd(),
                    ErrorAttempts_);
                return TChunkReadResult{
                    .Records = {},
                    .Finished = true,
                };
            }

            auto result = Phase_ == EPhase::Active
                ? RunActivePhaseIteration()
                : RunFinalPhaseIteration();

            if (result) {
                return std::move(*result);
            }
        }

        YT_LOG_DEBUG(
            "Failed to read distributed chunk session records; attempts exhausted "
            "(MaxReadAttempts: %v, Cursor: %v, Phase: %v)",
            Config_->MaxReadAttempts,
            Cursor_,
            Phase_);

        THROW_ERROR_EXCEPTION("Read attempts exhausted")
            << InnerErrors_;
    }

    // Increments the error budget and sleeps.
    // Callers push any relevant errors into InnerErrors_ before calling.
    void AccountError()
    {
        ++ErrorAttempts_;
        Statistics_->ErrorAttemptCount.fetch_add(1, std::memory_order::relaxed);
        ErrorBackoffStrategy_.Next();
        TDelayedExecutor::WaitForDuration(ErrorBackoffStrategy_.GetBackoff());
    }

    void TryFetchChunkRecordCount()
    {
        YT_LOG_DEBUG(
            "Computing distributed chunk session chunk quorum info "
            "(ReadQuorum: %v, ReplicaCount: %v)",
            ReadQuorum_,
            Replicas_.size());

        Statistics_->ComputeQuorumInfoCount.fetch_add(1, std::memory_order::relaxed);

        auto quorumInfoOrError = WaitFor(ComputeQuorumInfo(
            ChunkId_,
            /*overlayed*/ false,
            ECodec::None,
            ReadQuorum_,
            Config_->ReplicaLagLimit,
            ToReplicaDescriptors(Replicas_),
            Config_->QuorumProbeTimeout,
            Client_->GetChannelFactory()));

        if (quorumInfoOrError.IsOK()) {
            ChunkRecordCount_ = quorumInfoOrError.Value().RowCount;
            Statistics_->ComputeQuorumInfoSuccessCount.fetch_add(1, std::memory_order::relaxed);
            YT_LOG_DEBUG(
                "Computed distributed chunk session chunk quorum info "
                "(ChunkRecordCount: %v)",
                *ChunkRecordCount_);
            return;
        }

        YT_LOG_DEBUG(
            quorumInfoOrError,
            "Failed to compute distributed chunk session chunk quorum info "
            "(ReadQuorum: %v, ReplicaCount: %v)",
            ReadQuorum_,
            Replicas_.size());

        auto replicaUpdateResult = UpdateReplicasFromMaster();
        if (!replicaUpdateResult.IsOK()) {
            InnerErrors_.push_back(replicaUpdateResult);
        }
        InnerErrors_.push_back(quorumInfoOrError);
        AccountError();
    }

    void HandleAllNoSuchChunk(std::vector<TError> probeErrors)
    {
        auto replicaUpdateResult = UpdateReplicasFromMaster();
        if (replicaUpdateResult.IsOK() && replicaUpdateResult.Value()) {
            YT_LOG_DEBUG(
                "Switching distributed chunk session reader to final phase after replica set update "
                "(ReplicaCount: %v)",
                Replicas_.size());
            Phase_ = EPhase::Final;
            return;
        }
        for (auto& error : probeErrors) {
            InnerErrors_.push_back(std::move(error));
        }
        if (!replicaUpdateResult.IsOK()) {
            InnerErrors_.push_back(replicaUpdateResult);
        }
        AccountError();
    }

    std::optional<TChunkReadResult> ReadFromSingleReplica(TChunkReplica replica, i64 endIndex)
    {
        YT_VERIFY(endIndex > Cursor_);

        int blockCount = static_cast<int>(std::min(
            endIndex - Cursor_,
            static_cast<i64>(std::numeric_limits<int>::max())));

        YT_LOG_DEBUG(
            "Reading distributed chunk session records from single replica "
            "(Replica: %v, Cursor: %v, EndIndex: %v, BlockCount: %v)",
            replica,
            Cursor_,
            endIndex,
            blockCount);

        auto reader = MakeUnderlyingReader(TChunkReplicaList{replica});

        auto blocksOrError = WaitFor(reader->ReadBlocks(
            MakeReadOptions(),
            CheckedIntegralCast<int>(Cursor_),
            blockCount));

        Statistics_->ReadBlocksCount.fetch_add(1, std::memory_order::relaxed);

        if (blocksOrError.IsOK() && !blocksOrError.Value().empty()) {
            std::vector<TSharedRef> records;
            records.reserve(blocksOrError.Value().size());
            for (auto& block : blocksOrError.Value()) {
                records.push_back(std::move(block.Data));
            }

            Cursor_ += std::ssize(records);

            YT_LOG_DEBUG(
                "Read distributed chunk session records from single replica "
                "(RecordCount: %v, Cursor: %v, Finished: %v)",
                records.size(),
                Cursor_,
                Cursor_ >= ComputeEffectiveEnd());

            return TChunkReadResult{
                .Records = std::move(records),
                .Finished = Cursor_ >= ComputeEffectiveEnd(),
            };
        }

        if (!blocksOrError.IsOK()) {
            YT_LOG_DEBUG(
                blocksOrError,
                "Failed to read distributed chunk session records from single replica "
                "(Replica: %v, Cursor: %v, BlockCount: %v)",
                replica,
                Cursor_,
                blockCount);
            InnerErrors_.push_back(blocksOrError);
        } else {
            YT_LOG_DEBUG(
                "Received empty distributed chunk session read response from single replica "
                "(Replica: %v, Cursor: %v, BlockCount: %v)",
                replica,
                Cursor_,
                blockCount);
        }

        AccountError();

        return std::nullopt;
    }

    std::optional<TChunkReadResult> RunActivePhaseIteration()
    {
        auto replicaProgress = GetReplicaProgress();

        YT_LOG_DEBUG(
            "Completed distributed chunk session reader replica progress query "
            "(Outcome: %v, Cursor: %v, PickedRowCount: %v)",
            replicaProgress.Outcome,
            Cursor_,
            replicaProgress.PickedRowCount);

        switch (replicaProgress.Outcome) {
            case EReplicaProgressOutcome::HasData: {
                i64 endIndex = replicaProgress.PickedRowCount;
                if (RangeEndIndex_) {
                    endIndex = std::min(endIndex, *RangeEndIndex_);
                }
                return ReadFromSingleReplica(*replicaProgress.PickedReplica, endIndex);
            }
            case EReplicaProgressOutcome::Sealed:
                YT_LOG_DEBUG("Switching distributed chunk session reader to final phase after replica reported seal");
                Phase_ = EPhase::Final;
                return std::nullopt;
            case EReplicaProgressOutcome::NoNewData:
                Statistics_->PollIterationCount.fetch_add(1, std::memory_order::relaxed);
                TDelayedExecutor::WaitForDuration(Config_->PollInterval);
                return std::nullopt;
            case EReplicaProgressOutcome::AllNoSuchChunk:
                HandleAllNoSuchChunk(std::move(replicaProgress.InnerErrors));
                return std::nullopt;
            case EReplicaProgressOutcome::AllOtherErrors:
                for (auto& innerError : replicaProgress.InnerErrors) {
                    InnerErrors_.push_back(std::move(innerError));
                }
                AccountError();
                return std::nullopt;
        }
        YT_UNREACHABLE();
    }

    std::optional<TChunkReadResult> RunFinalPhaseIteration()
    {
        if (!ChunkRecordCount_) {
            TryFetchChunkRecordCount();
            return std::nullopt;
        }

        int blockCount = static_cast<int>(std::min(
            ComputeEffectiveEnd() - Cursor_,
            static_cast<i64>(std::numeric_limits<int>::max())));

        YT_LOG_DEBUG(
            "Reading distributed chunk session records from replica set "
            "(Cursor: %v, EffectiveEnd: %v, BlockCount: %v, ReplicaCount: %v)",
            Cursor_,
            ComputeEffectiveEnd(),
            blockCount,
            Replicas_.size());

        auto reader = MakeUnderlyingReader(Replicas_);
        auto blocksOrError = WaitFor(reader->ReadBlocks(
            MakeReadOptions(),
            CheckedIntegralCast<int>(Cursor_),
            blockCount));

        Statistics_->ReadBlocksCount.fetch_add(1, std::memory_order::relaxed);

        if (blocksOrError.IsOK() && !blocksOrError.Value().empty()) {
            std::vector<TSharedRef> records;
            records.reserve(blocksOrError.Value().size());
            for (const auto& block : blocksOrError.Value()) {
                records.push_back(block.Data);
            }

            Cursor_ += std::ssize(records);

            YT_LOG_DEBUG(
                "Read distributed chunk session records from replica set "
                "(RecordCount: %v, Cursor: %v, Finished: %v)",
                records.size(),
                Cursor_,
                Cursor_ >= ComputeEffectiveEnd());

            return TChunkReadResult{
                .Records = std::move(records),
                .Finished = Cursor_ >= ComputeEffectiveEnd(),
            };
        }

        if (!blocksOrError.IsOK()) {
            YT_LOG_DEBUG(
                blocksOrError,
                "Failed to read distributed chunk session records from replica set "
                "(Cursor: %v, BlockCount: %v, ReplicaCount: %v)",
                Cursor_,
                blockCount,
                Replicas_.size());
        } else {
            YT_LOG_DEBUG(
                "Received empty distributed chunk session read response from replica set "
                "(Cursor: %v, BlockCount: %v, ReplicaCount: %v)",
                Cursor_,
                blockCount,
                Replicas_.size());
        }

        auto replicaUpdateResult = UpdateReplicasFromMaster();

        if (!replicaUpdateResult.IsOK()) {
            InnerErrors_.push_back(replicaUpdateResult);
        }

        if (!blocksOrError.IsOK()) {
            InnerErrors_.push_back(blocksOrError);
        }

        AccountError();

        return std::nullopt;
    }

    std::vector<TChunkReplicaDescriptor> ToReplicaDescriptors(
        const TChunkReplicaList& replicas) const
    {
        const auto& nodeDirectory = Client_->GetNativeConnection()->GetNodeDirectory();

        std::vector<TChunkReplicaDescriptor> result;
        result.reserve(replicas.size());
        for (auto replica : replicas) {
            result.push_back({
                .NodeDescriptor = nodeDirectory->GetDescriptor(replica),
                .ReplicaIndex = replica.GetReplicaIndex(),
            });
        }

        return result;
    }

    IChunkReaderPtr MakeUnderlyingReader(const TChunkReplicaList& seedReplicas)
    {
        auto options = New<TRemoteReaderOptions>();

        options->AllowFetchingSeedsFromMaster = false;

        return CreateReplicationReader(
            Config_->UnderlyingReaderConfig,
            std::move(options),
            ChunkReaderHost_,
            ChunkId_,
            seedReplicas);
    }

    static IChunkReader::TReadBlocksOptions MakeReadOptions()
    {
        return IChunkReader::TReadBlocksOptions{
            .ClientOptions = {
                .ReadSessionId = TGuid::Create(),
            },
        };
    }

    TReplicaProgress GetReplicaProgress()
    {
        Statistics_->ActiveReplicaProgressQueryCount.fetch_add(1, std::memory_order::relaxed);

        const auto& connection = Client_->GetNativeConnection();
        const auto& nodeDirectory = connection->GetNodeDirectory();
        const auto& channelFactory = Client_->GetChannelFactory();
        const auto& networks = connection->GetNetworks();

        std::vector<TFuture<TDataNodeServiceProxy::TRspGetChunkMetaPtr>> futures;
        futures.reserve(Replicas_.size());
        for (const auto& replica : Replicas_) {
            const auto& descriptor = nodeDirectory->GetDescriptor(replica);
            TDataNodeServiceProxy proxy(
                channelFactory->CreateChannel(descriptor.GetAddressOrThrow(networks)));
            proxy.SetDefaultTimeout(Config_->ProbeTimeout);
            auto req = proxy.GetChunkMeta();
            ToProto(req->mutable_chunk_id(), ChunkId_);
            req->set_all_extension_tags(false);
            req->add_extension_tags(TProtoExtensionTag<TMiscExt>::Value);
            NRpc::SetRequestWorkloadDescriptor(req, TWorkloadDescriptor(EWorkloadCategory::UserBatch));
            futures.push_back(req->Invoke());
        }

        auto result = WaitFor(AnySetMatching(
            std::move(futures),
            [cursor = Cursor_] (const TDataNodeServiceProxy::TErrorOrRspGetChunkMetaPtr& rspOrError) noexcept {
                if (!rspOrError.IsOK()) {
                    return false;
                }
                auto miscExt = FindProtoExtension<TMiscExt>(
                    rspOrError.Value()->chunk_meta().extensions());
                return miscExt && (miscExt->sealed() || miscExt->row_count() > cursor);
            }))
            .ValueOrThrow();

        if (result.MatchingIndex) {
            const auto replicaIndex = *result.MatchingIndex;
            auto miscExt = GetProtoExtension<TMiscExt>(
                result.Results[replicaIndex]->Value()->chunk_meta().extensions());
            if (miscExt.sealed()) {
                Statistics_->SealedDetectedCount.fetch_add(1, std::memory_order::relaxed);
                return TReplicaProgress{
                    .Outcome = EReplicaProgressOutcome::Sealed,
                };
            }
            return TReplicaProgress{
                .Outcome = EReplicaProgressOutcome::HasData,
                .PickedReplica = Replicas_[replicaIndex],
                .PickedRowCount = miscExt.row_count(),
            };
        }

        // All responses settled with no useful data — classify by error type.
        int successCount = 0;
        int noSuchChunkCount = 0;
        std::vector<TError> innerErrors;
        for (const auto& rspOrError : result.Results) {
            if (rspOrError->IsOK()) {
                ++successCount;
            } else {
                if (rspOrError->FindMatching(NChunkClient::EErrorCode::NoSuchChunk)) {
                    ++noSuchChunkCount;
                }
                innerErrors.push_back(*rspOrError);
            }
        }

        EReplicaProgressOutcome outcome;
        if (successCount > 0) {
            outcome = EReplicaProgressOutcome::NoNewData;
        } else if (noSuchChunkCount == std::ssize(result.Results)) {
            outcome = EReplicaProgressOutcome::AllNoSuchChunk;
        } else {
            outcome = EReplicaProgressOutcome::AllOtherErrors;
        }

        return TReplicaProgress{
            .Outcome = outcome,
            .InnerErrors = std::move(innerErrors),
        };
    }

    // NB(apollo1321): this method can overload master at cluster scale.
    // v1 callers don't gate it: NoSuchChunk gating, per-reader rate limit,
    // and a separate master backoff are deferred.
    TErrorOr<bool> UpdateReplicasFromMaster()
    {
        YT_LOG_DEBUG(
            "Updating distributed chunk session reader replicas from master (ReplicaCount: %v)",
            Replicas_.size());

        IChannelPtr channel;
        try {
            channel = Client_->GetMasterChannelOrThrow(
                EMasterChannelKind::Follower,
                CellTagFromId(ChunkId_));
        } catch (const std::exception& ex) {
            YT_LOG_DEBUG(
                TError(ex),
                "Failed to acquire master channel while updating distributed chunk session reader replicas");
            return TError("Failed to acquire master channel for chunk %v", ChunkId_)
                << TError(ex);
        }

        TChunkServiceProxy proxy(std::move(channel));
        proxy.SetDefaultTimeout(Config_->RefreshTimeout);
        auto req = proxy.LocateChunks();
        ToProto(req->add_subrequests(), ChunkId_);

        auto rspOrError = WaitFor(req->Invoke());
        if (!rspOrError.IsOK()) {
            YT_LOG_DEBUG(
                rspOrError,
                "Failed to locate chunk while updating distributed chunk session reader replicas");
            return TError("Failed to locate chunk %v on master", ChunkId_)
                << rspOrError;
        }
        const auto& rsp = rspOrError.Value();

        Client_->GetNativeConnection()->GetNodeDirectory()->MergeFrom(rsp->node_directory());

        YT_VERIFY(rsp->subresponses_size() == 1);
        const auto& subresponse = rsp->subresponses(0);
        if (subresponse.missing()) {
            YT_LOG_DEBUG("Chunk is missing on master while updating distributed chunk session reader replicas");
            return TError(NChunkClient::EErrorCode::NoSuchChunk,
                "Chunk %v is missing on master", ChunkId_);
        }

        auto newReplicas = FromProto<TChunkReplicaList>(subresponse.replicas());

        auto toSortedKeys = [] (const TChunkReplicaList& replicas) {
            std::vector<std::pair<TNodeId, int>> result;
            result.reserve(replicas.size());
            for (const auto& replica : replicas) {
                result.emplace_back(replica.GetNodeId(), replica.GetReplicaIndex());
            }
            std::sort(result.begin(), result.end());
            return result;
        };

        bool replicasChanged = toSortedKeys(newReplicas) != toSortedKeys(Replicas_);
        int oldReplicaCount = std::ssize(Replicas_);

        Replicas_ = std::move(newReplicas);

        Statistics_->MasterRefreshCount.fetch_add(1, std::memory_order::relaxed);

        YT_LOG_DEBUG(
            "Updated distributed chunk session reader replicas from master "
            "(ReplicasChanged: %v, OldReplicaCount: %v, NewReplicaCount: %v)",
            replicasChanged,
            oldReplicaCount,
            Replicas_.size());

        return replicasChanged;
    }
};

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionReaderPtr CreateDistributedChunkSessionReader(
    TDistributedChunkSessionReaderConfigPtr config,
    IClientPtr client,
    TChunkReaderHostPtr chunkReaderHost,
    TChunkId chunkId,
    TChunkReplicaList replicas,
    int readQuorum,
    i64 startRecordIndex,
    std::optional<i64> rangeEndRecordIndex,
    IInvokerPtr invoker)
{
    return New<TDistributedChunkSessionReader>(
        std::move(config),
        std::move(client),
        std::move(chunkReaderHost),
        chunkId,
        std::move(replicas),
        readQuorum,
        startRecordIndex,
        rangeEndRecordIndex,
        std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionClient
