#include "private.h"
#include "shuffle_controller.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/session_pool.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/helpers.h>

#include <yt/yt/ytlib/push_based_shuffle_client/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/client/api/config.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/serialized_invoker.h>

namespace NYT::NShuffleServer {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NDistributedChunkSessionClient;
using namespace NPushBasedShuffleClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

constinit const auto Logger = ShuffleServiceLogger;

////////////////////////////////////////////////////////////////////////////////

class TPullBasedShuffleController
    : public IPullBasedShuffleController
{
public:
    TPullBasedShuffleController(int partitionCount, IInvokerPtr invoker, ITransactionPtr transaction)
        : PartitionCount_(partitionCount)
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Transaction_(std::move(transaction))
    { }

    TFuture<void> RegisterChunks(
        std::vector<TInputChunkPtr> chunks,
        std::optional<int> logicalWriterIndex,
        bool overwriteExistingWriterData) override
    {
        return BIND(
            &TPullBasedShuffleController::DoRegisterChunks,
            MakeStrong(this),
            Passed(std::move(chunks)),
            logicalWriterIndex,
            overwriteExistingWriterData)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    TFuture<std::vector<TInputChunkSlicePtr>> FetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> logicalWriterIndexRange) override
    {
        return BIND(
            &TPullBasedShuffleController::DoFetchChunks,
            MakeStrong(this),
            partitionIndex,
            logicalWriterIndexRange)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    struct TWriterChunk
    {
        TInputChunkPtr Chunk;
        std::optional<int> Epoch;
        std::optional<int> LogicalWriterIndex;
    };

    const int PartitionCount_;
    const IInvokerPtr SerializedInvoker_;
    //! Held only to keep the shuffle transaction (and thus the registered chunks)
    //! alive for the shuffle's lifetime; not otherwise used by the pull path.
    const ITransactionPtr Transaction_;

    std::vector<TWriterChunk> Chunks_;

    std::map<int, std::vector<int>> ChunkIndicesByLogicalWriterIndex_;
    std::unordered_map<int, int> EpochByLogicalWriterIndex_;

    void DoRegisterChunks(
        std::vector<TInputChunkPtr> chunks,
        std::optional<int> logicalWriterIndex,
        bool overwriteExistingWriterData)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::optional<int> currentEpoch;
        if (logicalWriterIndex) {
            if (overwriteExistingWriterData) {
                auto it = EpochByLogicalWriterIndex_.find(*logicalWriterIndex);
                if (it == EpochByLogicalWriterIndex_.end()) {
                    it = EpochByLogicalWriterIndex_.insert({*logicalWriterIndex, 0}).first;
                }
                YT_VERIFY(it->second < std::numeric_limits<int>::max());
                ++it->second;
            }
            currentEpoch = EpochByLogicalWriterIndex_[*logicalWriterIndex];
        }

        Chunks_.reserve(Chunks_.size() + chunks.size());
        for (auto& chunk : chunks) {
            const auto* partitionsExt = chunk->PartitionsExt().get();
            YT_VERIFY(partitionsExt);
            YT_VERIFY(partitionsExt->row_counts_size() == PartitionCount_);
            YT_VERIFY(partitionsExt->uncompressed_data_sizes_size() == PartitionCount_);

            if (logicalWriterIndex) {
                ChunkIndicesByLogicalWriterIndex_[*logicalWriterIndex].push_back(std::ssize(Chunks_));
            }

            Chunks_.push_back(TWriterChunk{
                .Chunk = std::move(chunk),
                .Epoch = currentEpoch,
                .LogicalWriterIndex = logicalWriterIndex,
            });
        }
    }

    std::vector<TInputChunkSlicePtr> DoFetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> logicalWriterIndexRange)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        THROW_ERROR_EXCEPTION_IF(
            partitionIndex < 0 || partitionIndex >= PartitionCount_,
            "Invalid partition index: expected a value between 0 and %v (exclusive), but received %v",
            PartitionCount_,
            partitionIndex);

        std::vector<TInputChunkSlicePtr> result;

        auto tryAddChunk = [&] (int index) {
            const auto& chunk = Chunks_[index];
            if (chunk.LogicalWriterIndex.has_value() &&
                chunk.Epoch < EpochByLogicalWriterIndex_[*chunk.LogicalWriterIndex])
            {
                return;
            }

            const auto* partitionsExt = chunk.Chunk->PartitionsExt().get();
            i64 rowCount = partitionsExt->row_counts()[partitionIndex];
            i64 dataSize = partitionsExt->uncompressed_data_sizes()[partitionIndex];
            i64 compressedDataSize = DivCeil(
                chunk.Chunk->GetCompressedDataSize(),
                chunk.Chunk->GetRowCount()) * rowCount;
            i64 uncompressedDataSize = DivCeil(
                chunk.Chunk->GetUncompressedDataSize(),
                chunk.Chunk->GetRowCount()) * rowCount;

            if (rowCount > 0) {
                result.push_back(CreateKeylessInputChunkSlice(chunk.Chunk));
                result.back()->OverrideSize(rowCount, dataSize, compressedDataSize, uncompressedDataSize);
            }
        };

        if (logicalWriterIndexRange) {
            for (auto it = ChunkIndicesByLogicalWriterIndex_.lower_bound(logicalWriterIndexRange->first);
                it != ChunkIndicesByLogicalWriterIndex_.end() && it->first < logicalWriterIndexRange->second;
                ++it)
            {
                for (int index : it->second) {
                    tryAddChunk(index);
                }
            }
        } else {
            for (int index = 0; index < std::ssize(Chunks_); ++index) {
                tryAddChunk(index);
            }
        }

        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPushBasedShuffleController
    : public IPushBasedShuffleController
{
public:
    TPushBasedShuffleController(
        int partitionCount,
        IInvokerPtr invoker,
        NNative::IClientPtr client,
        ITransactionPtr transaction,
        std::string account,
        std::string medium,
        int replicationFactor,
        TPushShuffleConfigPtr pushConfig)
        : PartitionCount_(partitionCount)
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , Transaction_(std::move(transaction))
        , Client_(std::move(client))
    {
        YT_TLOG_DEBUG("Initializing push-based shuffle")
            .With("Account", account)
            .With("Medium", medium)
            .With("ReplicationFactor", replicationFactor);

        // SetDefaults() is required here; New<>() does not apply YSON defaults.
        auto writerOptions = New<TJournalChunkWriterOptions>();
        writerOptions->SetDefaults();
        writerOptions->ReplicationFactor = replicationFactor;
        // Derive read/write quorums from the replication factor so that the read
        // and write quorums always intersect; the default 2/2 quorums are unsafe
        // for replication factors other than 3.
        auto quorums = ComputeDefaultJournalQuorums(replicationFactor);
        writerOptions->ReadQuorum = quorums.ReadQuorum;
        writerOptions->WriteQuorum = quorums.WriteQuorum;

        // The journal writer config (sequencer batch/flush knobs) and pool config (e.g.
        // max_active_sessions_per_slot) come from the handle's push config when set; otherwise
        // defaults. The quorums above stay derived from the replication factor regardless.
        TJournalChunkWriterConfigPtr writerConfig;
        TDistributedChunkSessionPoolConfigPtr poolConfig;
        if (pushConfig) {
            writerConfig = pushConfig->JournalWriterConfig;
            poolConfig = pushConfig->SessionPoolConfig;
        } else {
            writerConfig = New<TJournalChunkWriterConfig>();
            writerConfig->SetDefaults();
            poolConfig = New<TDistributedChunkSessionPoolConfig>();
            poolConfig->SetDefaults();
        }
        auto controllerConfig = New<TDistributedChunkSessionControllerConfig>();
        controllerConfig->SetDefaults();
        controllerConfig->Account = std::move(account);
        controllerConfig->MediumName = std::move(medium);

        Pool_ = CreateDistributedChunkSessionPool(
            Client_,
            std::move(poolConfig),
            std::move(controllerConfig),
            Transaction_->GetId(),
            std::move(writerOptions),
            std::move(writerConfig),
            SerializedInvoker_,
            /*sealMonitor*/ nullptr);

        for (int partitionIndex = 0; partitionIndex < PartitionCount_; ++partitionIndex) {
            Pool_->GetSession(partitionIndex)
                .Subscribe(BIND_NO_PROPAGATE([partitionIndex] (const TErrorOr<TSessionDescriptor>& sessionOrError) {
                    if (!sessionOrError.IsOK()) {
                        YT_TLOG_DEBUG("Failed to eagerly start partition write session")
                            .With("PartitionIndex", partitionIndex)
                            .With(sessionOrError);
                    }
                }));
        }
    }

    TFuture<TWriterRegistration> RegisterWriter(
        std::optional<int> logicalWriterIndex,
        bool overwriteExistingWriterData) override
    {
        return BIND(
            &TPushBasedShuffleController::DoRegisterWriter,
            MakeStrong(this),
            logicalWriterIndex,
            overwriteExistingWriterData)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    TFuture<TSessionDescriptor> GetPartitionWriteSession(
        int partitionIndex,
        std::optional<TSessionId> excludedSessionId) override
    {
        return BIND(
            &TPushBasedShuffleController::DoGetPartitionWriteSession,
            MakeStrong(this),
            partitionIndex,
            excludedSessionId)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    TFuture<TPushBasedFetchResult> FetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> logicalWriterIndexRange) override
    {
        return BIND(
            &TPushBasedShuffleController::DoFetchChunks,
            MakeStrong(this),
            partitionIndex,
            logicalWriterIndexRange)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    const int PartitionCount_;
    const IInvokerPtr SerializedInvoker_;
    const ITransactionPtr Transaction_;
    const NNative::IClientPtr Client_;

    // Each partition maps 1:1 to a pool slot cookie equal to its index.
    IDistributedChunkSessionPoolPtr Pool_;
    i32 NextWriterId_ = 0;
    THashMap<i32, std::optional<int>> LogicalWriterIndexByWriterId_;
    // All writer ids ever allocated per logical writer index — needed to
    // invalidate the entire history of a logical writer on overwrite.
    THashMap<int, std::vector<i32>> WriterIdsByLogicalWriterIndex_;
    THashSet<i32> ValidWriterIds_;
    bool ReadPhaseStarted_ = false;

    TFuture<TWriterRegistration> DoRegisterWriter(
        std::optional<int> logicalWriterIndex,
        bool overwriteExistingWriterData)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (ReadPhaseStarted_) {
            THROW_ERROR_EXCEPTION("Shuffle read phase has started; cannot register a new writer");
        }
        if (overwriteExistingWriterData && !logicalWriterIndex) {
            THROW_ERROR_EXCEPTION(
                "Logical writer index must be set when overwrite existing writer data option is enabled");
        }

        i32 writerId = NextWriterId_++;
        YT_VERIFY(NextWriterId_ > 0);

        LogicalWriterIndexByWriterId_[writerId] = logicalWriterIndex;
        if (logicalWriterIndex && overwriteExistingWriterData) {
            auto it = WriterIdsByLogicalWriterIndex_.find(*logicalWriterIndex);
            if (it != WriterIdsByLogicalWriterIndex_.end()) {
                for (i32 priorWriterId : it->second) {
                    ValidWriterIds_.erase(priorWriterId);
                }
            }
        }
        if (logicalWriterIndex) {
            WriterIdsByLogicalWriterIndex_[*logicalWriterIndex].push_back(writerId);
        }
        ValidWriterIds_.insert(writerId);

        return Pool_->GetReadySessions()
            .Apply(BIND_NO_PROPAGATE([writerId] (std::vector<TReadySession> readySessions) {
                return TWriterRegistration{
                    .WriterId = writerId,
                    .ReadySessions = std::move(readySessions),
                };
            }));
    }

    TFuture<TSessionDescriptor> DoGetPartitionWriteSession(
        int partitionIndex,
        std::optional<TSessionId> excludedSessionId)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        THROW_ERROR_EXCEPTION_IF(
            partitionIndex < 0 || partitionIndex >= PartitionCount_,
            "Invalid partition index: expected a value between 0 and %v (exclusive), but received %v",
            PartitionCount_,
            partitionIndex);

        if (ReadPhaseStarted_) {
            THROW_ERROR_EXCEPTION("Shuffle read phase has started; new writes are not allowed");
        }

        return Pool_->GetSession(partitionIndex, excludedSessionId);
    }

    TFuture<TPushBasedFetchResult> DoFetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> logicalWriterIndexRange)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        THROW_ERROR_EXCEPTION_IF(
            partitionIndex < 0 || partitionIndex >= PartitionCount_,
            "Invalid partition index: expected a value between 0 and %v (exclusive), but received %v",
            PartitionCount_,
            partitionIndex);

        if (!ReadPhaseStarted_) {
            ReadPhaseStarted_ = true;
            for (int partition = 0; partition < PartitionCount_; ++partition) {
                Pool_->FinalizeSlot(partition);
            }
        }

        auto validWriterIds = ComputeValidWriterIdsForRange(logicalWriterIndexRange);

        return Pool_->GetSlotChunks(partitionIndex)
            .Apply(BIND_NO_PROPAGATE(
                &TPushBasedShuffleController::MakeFetchResult,
                Passed(std::move(validWriterIds))));
    }

    static TPushBasedFetchResult MakeFetchResult(
        std::vector<i32> validWriterIds,
        std::vector<TSlotChunkInfo> chunkInfos)
    {
        return TPushBasedFetchResult{
            .Chunks = std::move(chunkInfos),
            .ValidWriterIds = std::move(validWriterIds),
        };
    }

    std::vector<i32> ComputeValidWriterIdsForRange(
        std::optional<std::pair<int, int>> logicalWriterIndexRange) const
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<i32> result;
        result.reserve(ValidWriterIds_.size());

        if (!logicalWriterIndexRange) {
            for (i32 writerId : ValidWriterIds_) {
                result.push_back(writerId);
            }
            return result;
        }

        for (i32 writerId : ValidWriterIds_) {
            auto it = LogicalWriterIndexByWriterId_.find(writerId);
            YT_VERIFY(it != LogicalWriterIndexByWriterId_.end());
            if (!it->second) {
                continue;
            }
            if (*it->second >= logicalWriterIndexRange->first && *it->second < logicalWriterIndexRange->second) {
                result.push_back(writerId);
            }
        }
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

IPullBasedShuffleControllerPtr CreatePullBasedShuffleController(
    int partitionCount,
    IInvokerPtr invoker,
    ITransactionPtr transaction)
{
    return New<TPullBasedShuffleController>(
        partitionCount,
        std::move(invoker),
        std::move(transaction));
}

IPushBasedShuffleControllerPtr CreatePushBasedShuffleController(
    int partitionCount,
    IInvokerPtr invoker,
    NApi::NNative::IClientPtr client,
    ITransactionPtr transaction,
    std::string account,
    std::string medium,
    int replicationFactor,
    TPushShuffleConfigPtr pushConfig)
{
    return New<TPushBasedShuffleController>(
        partitionCount,
        std::move(invoker),
        std::move(client),
        std::move(transaction),
        std::move(account),
        std::move(medium),
        replicationFactor,
        std::move(pushConfig));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NShuffleServer
