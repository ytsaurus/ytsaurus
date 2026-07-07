#include "private.h"
#include "shuffle_controller.h"

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>

#include <yt/yt/ytlib/distributed_chunk_session_client/config.h>
#include <yt/yt/ytlib/distributed_chunk_session_client/distributed_chunk_session_pool.h>
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
        std::optional<int> writerIndex,
        bool overwriteExistingWriterData) override
    {
        return BIND(
            &TPullBasedShuffleController::DoRegisterChunks,
            MakeStrong(this),
            Passed(std::move(chunks)),
            writerIndex,
            overwriteExistingWriterData)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

    TFuture<std::vector<TInputChunkSlicePtr>> FetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> writerIndexRange) override
    {
        return BIND(
            &TPullBasedShuffleController::DoFetchChunks,
            MakeStrong(this),
            partitionIndex,
            writerIndexRange)
            .AsyncVia(SerializedInvoker_)
            .Run();
    }

private:
    struct TWriterChunk
    {
        TInputChunkPtr Chunk;
        std::optional<int> Epoch;
        std::optional<int> WriterIndex;
    };

    const int PartitionCount_;
    const IInvokerPtr SerializedInvoker_;
    //! Held only to keep the shuffle transaction (and thus the registered chunks)
    //! alive for the shuffle's lifetime; not otherwise used by the pull path.
    const ITransactionPtr Transaction_;

    std::vector<TWriterChunk> Chunks_;

    std::map<int, std::vector<int>> WriterIndexToChunkIndices_;
    std::unordered_map<int, int> WriterIndexToEpoch_;

    void DoRegisterChunks(std::vector<TInputChunkPtr> chunks, std::optional<int> writerIndex, bool overwriteExistingWriterData)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::optional<int> currentEpoch;
        if (writerIndex) {
            if (overwriteExistingWriterData) {
                auto it = WriterIndexToEpoch_.find(*writerIndex);
                if (it == WriterIndexToEpoch_.end()) {
                    it = WriterIndexToEpoch_.insert({*writerIndex, 0}).first;
                }
                YT_VERIFY(it->second < std::numeric_limits<int>::max());
                ++it->second;
            }
            currentEpoch = WriterIndexToEpoch_[*writerIndex];
        }

        Chunks_.reserve(Chunks_.size() + chunks.size());
        for (auto& chunk : chunks) {
            const auto* partitionsExt = chunk->PartitionsExt().get();
            YT_VERIFY(partitionsExt);
            YT_VERIFY(partitionsExt->row_counts_size() == PartitionCount_);
            YT_VERIFY(partitionsExt->uncompressed_data_sizes_size() == PartitionCount_);

            if (writerIndex) {
                WriterIndexToChunkIndices_[*writerIndex].push_back(std::ssize(Chunks_));
            }

            Chunks_.push_back(TWriterChunk{
                .Chunk = std::move(chunk),
                .Epoch = currentEpoch,
                .WriterIndex = writerIndex,
            });
        }
    }

    std::vector<TInputChunkSlicePtr> DoFetchChunks(
        int partitionIndex,
        std::optional<std::pair<int, int>> writerIndexRange)
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
            if (chunk.WriterIndex.has_value() && chunk.Epoch < WriterIndexToEpoch_[*chunk.WriterIndex]) {
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
                result.push_back(New<TInputChunkSlice>(chunk.Chunk));
                result.back()->OverrideSize(rowCount, dataSize, compressedDataSize, uncompressedDataSize);
            }
        };

        if (writerIndexRange) {
            for (auto it = WriterIndexToChunkIndices_.lower_bound(writerIndexRange->first);
                it != WriterIndexToChunkIndices_.end() && it->first < writerIndexRange->second;
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
        YT_LOG_DEBUG(
            "Initializing push-based shuffle (Account: %v, Medium: %v, ReplicationFactor: %v)",
            account,
            medium,
            replicationFactor);

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
            SerializedInvoker_);

        for (int partitionIndex = 0; partitionIndex < PartitionCount_; ++partitionIndex) {
            Pool_->GetSession(partitionIndex)
                .Subscribe(BIND_NO_PROPAGATE([partitionIndex] (const TErrorOr<TSessionDescriptor>& sessionOrError) {
                    if (!sessionOrError.IsOK()) {
                        YT_LOG_DEBUG(
                            sessionOrError,
                            "Failed to eagerly start partition write session (PartitionIndex: %v)",
                            partitionIndex);
                    }
                }));
        }
    }

    TFuture<TMapperRegistration> RegisterMapper(
        std::optional<int> writerIndex,
        bool overwriteExistingWriterData) override
    {
        return BIND(
            &TPushBasedShuffleController::DoRegisterMapper,
            MakeStrong(this),
            writerIndex,
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
        std::optional<std::pair<int, int>> writerIndexRange) override
    {
        return BIND(
            &TPushBasedShuffleController::DoFetchChunks,
            MakeStrong(this),
            partitionIndex,
            writerIndexRange)
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
    i32 NextMapperId_ = 0;
    THashMap<i32, std::optional<int>> MapperIdToWriterIndex_;
    // All mapper ids ever allocated per writer index — needed to invalidate the
    // entire history of a writer index on overwrite (not just the latest).
    THashMap<int, std::vector<i32>> WriterIndexToMapperIds_;
    THashSet<i32> ValidMapperIds_;
    bool ReadPhaseStarted_ = false;

    TFuture<TMapperRegistration> DoRegisterMapper(std::optional<int> writerIndex, bool overwriteExistingWriterData)
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        if (ReadPhaseStarted_) {
            THROW_ERROR_EXCEPTION("Shuffle read phase has started; cannot register a new mapper");
        }
        if (overwriteExistingWriterData && !writerIndex) {
            THROW_ERROR_EXCEPTION("Writer index must be set when overwrite existing writer data option is enabled");
        }

        i32 mapperId = NextMapperId_++;
        YT_VERIFY(NextMapperId_ > 0);

        MapperIdToWriterIndex_[mapperId] = writerIndex;
        if (writerIndex && overwriteExistingWriterData) {
            auto it = WriterIndexToMapperIds_.find(*writerIndex);
            if (it != WriterIndexToMapperIds_.end()) {
                for (i32 priorMapperId : it->second) {
                    ValidMapperIds_.erase(priorMapperId);
                }
            }
        }
        if (writerIndex) {
            WriterIndexToMapperIds_[*writerIndex].push_back(mapperId);
        }
        ValidMapperIds_.insert(mapperId);

        return Pool_->GetReadySessions()
            .Apply(BIND_NO_PROPAGATE([mapperId] (std::vector<TReadySession> readySessions) {
                return TMapperRegistration{
                    .MapperId = mapperId,
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
        std::optional<std::pair<int, int>> writerIndexRange)
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

        auto validMapperIds = ComputeValidMapperIdsForRange(writerIndexRange);

        return Pool_->GetSlotChunks(partitionIndex)
            .Apply(BIND_NO_PROPAGATE(
                &TPushBasedShuffleController::MakeFetchResult,
                Passed(std::move(validMapperIds))));
    }

    static TPushBasedFetchResult MakeFetchResult(
        std::vector<i32> validMapperIds,
        std::vector<TSlotChunkInfo> chunkInfos)
    {
        return TPushBasedFetchResult{
            .Chunks = std::move(chunkInfos),
            .ValidMapperIds = std::move(validMapperIds),
        };
    }

    std::vector<i32> ComputeValidMapperIdsForRange(
        std::optional<std::pair<int, int>> writerIndexRange) const
    {
        YT_ASSERT_INVOKER_AFFINITY(SerializedInvoker_);

        std::vector<i32> result;
        result.reserve(ValidMapperIds_.size());

        if (!writerIndexRange) {
            for (i32 mapperId : ValidMapperIds_) {
                result.push_back(mapperId);
            }
            return result;
        }

        for (i32 mapperId : ValidMapperIds_) {
            auto it = MapperIdToWriterIndex_.find(mapperId);
            YT_VERIFY(it != MapperIdToWriterIndex_.end());
            if (!it->second) {
                continue;
            }
            if (*it->second >= writerIndexRange->first && *it->second < writerIndexRange->second) {
                result.push_back(mapperId);
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
