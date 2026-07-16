#include "compact_output_store.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>
#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/message_batch.h>
#include <yt/yt/flow/library/cpp/common/message_migration.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/stream_spec_storage.h>

#include <yt/yt/flow/library/cpp/tables/compact_output_messages.h>
#include <yt/yt/flow/library/cpp/tables/compact_partition_output_messages.h>

#include <yt/yt/client/table_client/public.h>

#include <library/cpp/containers/absl/flat_hash_map.h>
#include <library/cpp/containers/absl/flat_hash_set.h>
#include <library/cpp/iterator/iterate_values.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace {

// Packed per-message handle into the in-memory chunk table: 44 bits of
// LocalChunkId (process-local, dense; assigned from NextLocalChunkId_) plus
// 20 bits of position within the chunk. Distinct from the YT chunk id (i64,
// derived from TimeProvider) stored in TChunkState::ChunkId.
struct TMessageLocation
{
    static constexpr int LocalChunkIdBits = 44;
    static constexpr int PositionInChunkBits = 20;

    ui64 LocalChunkId : LocalChunkIdBits = 0;
    ui64 PositionInChunk : PositionInChunkBits = 0;
};

static_assert(sizeof(TMessageLocation) == 8);

constexpr i64 MaxChunkMessageCountLimit = 1LL << TMessageLocation::PositionInChunkBits;

// Bounded by YT's per-value string-column limit; pre-compression checks are stricter.
constexpr i64 MaxChunkDataSize = NTableClient::MaxStringValueLength;

// YT caps transactions at 100'000 modified rows; keep well under. The bound
// applies to the number of YT-side row touches the async drain may schedule
// (mask updates + erases), not to the number of UnregisterImpl calls — most
// of which only mutate in-memory state without producing a row modification.
constexpr int MaxDirtyChunksPerTransaction = 20'000;

bool MaskGet(const std::string& mask, int position)
{
    auto byte = static_cast<size_t>(position) / 8;
    if (byte >= mask.size()) {
        return false;
    }
    return (static_cast<unsigned char>(mask[byte]) & (1u << (position % 8))) != 0;
}

void MaskSet(std::string& mask, int position)
{
    auto byte = static_cast<size_t>(position) / 8;
    if (byte >= mask.size()) {
        mask.resize(byte + 1, '\0');
    }
    mask[byte] = static_cast<char>(static_cast<unsigned char>(mask[byte]) | (1u << (position % 8)));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCompactOutputStore
    : public IOutputStore
{
public:
    TCompactOutputStore(TCompactOutputStoreContextPtr context, TDynamicOutputStoreSpecPtr dynamicSpec)
        : Context_(std::move(context))
        , DynamicSpec_(std::move(dynamicSpec))
        , KeyTable_(Context_->CompactOutputMessagesTable)
        , PartitionTable_(Context_->CompactPartitionOutputMessagesTable)
        , TimeProvider_(Context_->TimeProvider)
        , Logger(Context_->Logger)
        , InflightStore_(New<TMultiInflightTracker>(
            Context_->Profiler.WithPrefix("/output_streams"),
            Context_->OutputStreamIds,
            Context_->WatermarkPercentileSpec,
            Context_->StreamLimitUsageStates))
    {
        YT_VERIFY(KeyTable_);
        YT_VERIFY(PartitionTable_);
        YT_VERIFY(TimeProvider_);
        ValidateSpec();
        KeyTable_->Reconfigure(DynamicSpec_->TableRequest);
        PartitionTable_->Reconfigure(DynamicSpec_->TableRequest);
    }

    void Reconfigure(TDynamicOutputStoreSpecPtr dynamicSpec) override
    {
        DynamicSpec_ = std::move(dynamicSpec);
        ValidateSpec();
        KeyTable_->Reconfigure(DynamicSpec_->TableRequest);
        PartitionTable_->Reconfigure(DynamicSpec_->TableRequest);
    }

    bool Contains(const TMessageMeta& message) const override
    {
        return InflightStore_->Contains(message);
    }

    void RegisterBatch(std::span<const TOutputMessageConstPtr> messages, bool persist = true) override
    {
        for (const auto& message : messages) {
            RegisterImpl(message, std::nullopt, persist, /*ensure*/ true);
        }
        InflightStore_->SyncCounters();
    }

    void TryRegisterBatch(std::span<const TOutputMessageConstPtr> messages, bool persist = true) override
    {
        for (const auto& message : messages) {
            RegisterImpl(message, std::nullopt, persist, /*ensure*/ false);
        }
        InflightStore_->SyncCounters();
    }

    void TryRegisterKeyedBatch(std::span<const TOutputMessageConstPtr> messages, const TKey& key, bool persist = true) override
    {
        if (!KeyStateLoaded_) {
            THROW_ERROR_EXCEPTION(
                "TryRegisterKeyedBatch called but key state was not loaded during Init; "
                "use Init(loadKeyState=true) before calling TryRegisterKeyedBatch");
        }
        for (const auto& message : messages) {
            RegisterImpl(message, key, persist, /*ensure*/ false);
        }
        InflightStore_->SyncCounters();
    }

    void TryUnregisterBatch(std::span<const TMessageMeta* const> metas) override
    {
        for (const auto* meta : metas) {
            UnregisterImpl(*meta, /*ensure*/ false);
        }
        InflightStore_->SyncCounters();
    }

    void AsyncUnregisterBatch(std::span<const TMessageMeta* const> metas) override
    {
        for (const auto* meta : metas) {
            YT_TLOG_DEBUG("MessageLifeCycle.CompactOutputStore: message was asynchronously unregistered")
                .With("MessageId", meta->MessageId)
                .With("StreamId", meta->StreamId);
            AsyncEraseQueue_.push_back(*meta);
        }
    }

    TFuture<std::vector<std::pair<TOutputMessageConstPtr, std::optional<TKey>>>> Init(bool loadKeyState) override
    {
        KeyStateLoaded_ = loadKeyState;
        auto loadPartitionChunks = PartitionTable_->LoadAll({.PartitionId = Context_->Partition->PartitionId});
        auto loadKeyedChunks = loadKeyState
            ? KeyTable_->LoadAll({
                  .ComputationId = Context_->Partition->ComputationId,
                  .LowerKey = Context_->Partition->LowerKey,
                  .ExactKey = Context_->Partition->SourceKey,
                  .UpperKey = Context_->Partition->UpperKey,
              })
            : MakeFuture<std::vector<NTables::ICompactOutputMessages::TChunk>>({});
        return BIND([weakThis = MakeWeak(this), loadPartitionChunks, loadKeyedChunks] () {
            auto partitionChunks = NConcurrency::WaitFor(loadPartitionChunks).ValueOrThrow();
            auto keyedChunks = NConcurrency::WaitFor(loadKeyedChunks).ValueOrThrow();
            auto strongThis = weakThis.Lock();
            THROW_ERROR_EXCEPTION_UNLESS(strongThis, "Interrupted");

            const auto& Logger = strongThis->Logger;
            NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
            YT_TLOG_DEBUG("Compact output messages loaded")
                .With("PartitionChunks", partitionChunks.size())
                .With("KeyedChunks", keyedChunks.size());

            const auto& specs = strongThis->Context_->StreamSpecStorage->GetStreamSpecs();

            // Parse each chunk's serialized batch once, up front, so the reserves below are exact
            // and LoadChunk does not have to re-parse.
            std::vector<std::deque<TMessage>> partitionMessages;
            partitionMessages.reserve(partitionChunks.size());
            std::vector<std::deque<TMessage>> keyedMessages;
            keyedMessages.reserve(keyedChunks.size());
            i64 totalMessages = 0;
            for (const auto& chunk : partitionChunks) {
                partitionMessages.push_back(ParseMessageBatch(chunk.Data, specs));
                totalMessages += std::ssize(partitionMessages.back());
            }
            for (const auto& chunk : keyedChunks) {
                keyedMessages.push_back(ParseMessageBatch(chunk.Data, specs));
                totalMessages += std::ssize(keyedMessages.back());
            }

            // Avoid rehashing during the bulk load below.
            const i64 totalChunks = std::ssize(partitionChunks) + std::ssize(keyedChunks);
            strongThis->Chunks_.reserve(totalChunks);
            strongThis->MessageLocations_.reserve(totalMessages);

            std::vector<std::pair<TOutputMessageConstPtr, std::optional<TKey>>> messages;
            messages.reserve(totalMessages);
            for (int index = 0; index < std::ssize(partitionChunks); ++index) {
                auto& chunk = partitionChunks[index];
                strongThis->LoadChunk(
                    chunk.Key.ChunkId,
                    std::nullopt,
                    chunk.Key.StreamId,
                    std::move(chunk.ProcessedMask),
                    std::move(partitionMessages[index]),
                    messages);
            }
            for (int index = 0; index < std::ssize(keyedChunks); ++index) {
                auto& chunk = keyedChunks[index];
                strongThis->LoadChunk(
                    chunk.Key.ChunkId,
                    chunk.Key.Key,
                    chunk.Key.StreamId,
                    std::move(chunk.ProcessedMask),
                    std::move(keyedMessages[index]),
                    messages);
            }
            // Typical single-stream, single-output-mode pipelines load already-sorted
            // messages: per-chunk pack order is MessageId order, and per-group chunks
            // are monotone in chunk_id. Skip the sort in that common case.
            auto getMessageId = [] (const auto& m) {
                return m.first->MessageId;
            };
            if (!IsSortedBy(messages, getMessageId)) {
                SortBy(messages, getMessageId);
            }

            YT_VERIFY(strongThis->ToPersist_.empty());
            YT_TLOG_DEBUG("Compact output store init completed")
                .With("MessageCount", std::ssize(messages));
            return messages;
        })
            .AsyncVia(GetCurrentInvoker())
            .Run();
    }

    void Sync(NApi::IDynamicTableTransactionPtr tx) override
    {
        // Three-phase flush: pack pending messages into chunks, then write new
        // chunks / refresh masks on existing ones / erase fully-processed ones.
        while (!AsyncEraseQueue_.empty() &&
            std::ssize(MaskDirtyChunks_) + std::ssize(ChunksToErase_) < MaxDirtyChunksPerTransaction)
        {
            UnregisterImpl(AsyncEraseQueue_.front(), /*ensure*/ true);
            AsyncEraseQueue_.pop_front();
        }
        InflightStore_->SyncCounters();

        PackPendingMessages();

        std::vector<NTables::ICompactPartitionOutputMessages::TChunk> partitionToWrite;
        std::vector<NTables::ICompactPartitionOutputMessages::TMaskUpdate> partitionMaskUpdates;
        std::vector<NTables::ICompactPartitionOutputMessages::TTableKey> partitionToErase;
        std::vector<NTables::ICompactOutputMessages::TChunk> keyedToWrite;
        std::vector<NTables::ICompactOutputMessages::TMaskUpdate> keyedMaskUpdates;
        std::vector<NTables::ICompactOutputMessages::TTableKey> keyedToErase;

        const auto& computationId = Context_->Partition->ComputationId;
        const auto& partitionId = Context_->Partition->PartitionId;

        // Invariant: every LocalChunkId in NewChunks_/MaskDirtyChunks_/ChunksToErase_ is in Chunks_.
        for (auto localChunkId : NewChunks_) {
            auto iter = Chunks_.find(localChunkId);
            YT_VERIFY(iter != Chunks_.end());
            auto& state = iter->second;
            if (state.Key) {
                keyedToWrite.push_back({
                    .Key = {
                        .ComputationId = computationId,
                        .Key = *state.Key,
                        .StreamId = state.StreamId,
                        .ChunkId = state.ChunkId,
                    },
                    .Data = std::move(state.SerializedBatch),
                    .ProcessedMask = state.ProcessedMask,
                });
            } else {
                partitionToWrite.push_back({
                    .Key = {
                        .PartitionId = partitionId,
                        .StreamId = state.StreamId,
                        .ChunkId = state.ChunkId,
                    },
                    .Data = std::move(state.SerializedBatch),
                    .ProcessedMask = state.ProcessedMask,
                });
            }
            state.SerializedBatch = {};
        }
        NewChunks_.clear();

        for (auto localChunkId : MaskDirtyChunks_) {
            auto iter = Chunks_.find(localChunkId);
            YT_VERIFY(iter != Chunks_.end());
            const auto& state = iter->second;
            if (state.Key) {
                keyedMaskUpdates.push_back({
                    .Key = {
                        .ComputationId = computationId,
                        .Key = *state.Key,
                        .StreamId = state.StreamId,
                        .ChunkId = state.ChunkId,
                    },
                    .ProcessedMask = state.ProcessedMask,
                });
            } else {
                partitionMaskUpdates.push_back({
                    .Key = {
                        .PartitionId = partitionId,
                        .StreamId = state.StreamId,
                        .ChunkId = state.ChunkId,
                    },
                    .ProcessedMask = state.ProcessedMask,
                });
            }
        }
        MaskDirtyChunks_.clear();

        for (auto localChunkId : ChunksToErase_) {
            auto iter = Chunks_.find(localChunkId);
            YT_VERIFY(iter != Chunks_.end());
            const auto& state = iter->second;
            if (state.Key) {
                keyedToErase.push_back({
                    .ComputationId = computationId,
                    .Key = *state.Key,
                    .StreamId = state.StreamId,
                    .ChunkId = state.ChunkId,
                });
            } else {
                partitionToErase.push_back({
                    .PartitionId = partitionId,
                    .StreamId = state.StreamId,
                    .ChunkId = state.ChunkId,
                });
            }
            Chunks_.erase(iter);
        }
        ChunksToErase_.clear();

        PartitionTable_->Write(tx, partitionToWrite, DynamicSpec_->CompressionCodec);
        PartitionTable_->UpdateMask(tx, partitionMaskUpdates);
        PartitionTable_->Erase(tx, partitionToErase);
        KeyTable_->Write(tx, keyedToWrite, DynamicSpec_->CompressionCodec);
        KeyTable_->UpdateMask(tx, keyedMaskUpdates);
        KeyTable_->Erase(tx, keyedToErase);
    }

    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() override
    {
        return InflightStore_->BuildInflights();
    }

    THashMap<TStreamId, std::pair<i64, i64>> GetCountAndByteSizes() override
    {
        return InflightStore_->GetCountAndByteSizes();
    }

private:
    void RegisterImpl(const TOutputMessageConstPtr& message, const std::optional<TKey>& key, bool persist, bool ensure)
    {
        if (!Context_->OutputStreamIds.contains(message->StreamId)) {
            THROW_ERROR_EXCEPTION("Unknown output stream %Qv during register",
                message->StreamId);
        }

        // Already persisted in a YT chunk — verify key matches, otherwise no-op.
        if (auto it = MessageLocations_.find(message->MessageId); it != MessageLocations_.end()) {
            auto chunkIt = Chunks_.find(it->second.LocalChunkId);
            YT_VERIFY(chunkIt != Chunks_.end());
            YT_VERIFY(chunkIt->second.Key == key);
            return;
        }
        // Already queued for the next Sync.
        if (auto it = ToPersist_.find(message); it != ToPersist_.end()) {
            YT_VERIFY(it->second == key);
            return;
        }
        // Was registered with a key but not yet queued — promote to ToPersist_ if persist=true.
        if (auto it = Keys_.find(message); it != Keys_.end()) {
            YT_VERIFY(key && *key == it->second);
            if (persist) {
                ToPersist_.emplace(message, std::move(it->second));
                Keys_.erase(it);
            }
            return;
        }
        // First-time registration.
        if (ensure) {
            InflightStore_->Register(message);
        } else {
            InflightStore_->TryRegister(message);
        }
        if (persist) {
            ToPersist_.emplace(message, key);
        } else if (key) {
            Keys_.emplace(message, *key);
        }
        YT_TLOG_DEBUG("MessageLifeCycle.CompactOutputStore: message was registered")
            .With("Key", key)
            .With("MessageId", message->MessageId)
            .With("StreamId", message->StreamId);
    }

    void UnregisterImpl(const TMessageMeta& messageMeta, bool ensure)
    {
        if (!Context_->OutputStreamIds.contains(messageMeta.StreamId)) {
            THROW_ERROR_EXCEPTION("Unknown output stream %Qv during unregister",
                messageMeta.StreamId);
        }
        if (ensure) {
            InflightStore_->Unregister(messageMeta);
        } else {
            InflightStore_->TryUnregister(messageMeta);
        }
        std::optional<TKey> key;
        if (auto it = Keys_.find(messageMeta.MessageId); it != Keys_.end()) {
            key = std::move(it->second);
            Keys_.erase(it);
        }

        if (ToPersist_.erase(messageMeta.MessageId) > 0) {
            // Was queued but never flushed.
        } else if (auto chunkIter = MessageLocations_.find(messageMeta.MessageId); chunkIter != MessageLocations_.end()) {
            const auto location = chunkIter->second;
            MessageLocations_.erase(chunkIter);
            const ui64 localChunkId = location.LocalChunkId;
            const ui32 position = location.PositionInChunk;
            auto stateIter = Chunks_.find(localChunkId);
            YT_VERIFY(stateIter != Chunks_.end());
            auto& state = stateIter->second;
            if (!MaskGet(state.ProcessedMask, position)) {
                MaskSet(state.ProcessedMask, position);
                if (!NewChunks_.contains(localChunkId)) {
                    MaskDirtyChunks_.insert(localChunkId);
                }
                if (--state.RemainingCount == 0) {
                    ChunksToErase_.insert(localChunkId);
                    NewChunks_.erase(localChunkId);
                    MaskDirtyChunks_.erase(localChunkId);
                }
            }
        }
        YT_TLOG_DEBUG("MessageLifeCycle.CompactOutputStore: message was unregistered")
            .With("Key", key)
            .With("MessageId", messageMeta.MessageId)
            .With("StreamId", messageMeta.StreamId);
    }

    void LoadChunk(
        i64 chunkId,
        std::optional<TKey> key,
        TStreamId streamId,
        std::string processedMask,
        std::deque<TMessage>&& chunkMessages,
        std::vector<std::pair<TOutputMessageConstPtr, std::optional<TKey>>>& messages)
    {
        const ui64 localChunkId = NextLocalChunkId_++;

        TChunkState state;
        state.Key = std::move(key);
        state.StreamId = streamId;
        state.ChunkId = chunkId;
        state.ProcessedMask = std::move(processedMask);

        // Loaded chunks only need mask updates — don't retain the serialized batch.
        const int messageCount = std::ssize(chunkMessages);
        YT_VERIFY(messageCount <= MaxChunkMessageCountLimit);
        for (int position = 0; position < messageCount; ++position) {
            if (MaskGet(state.ProcessedMask, position)) {
                continue;
            }
            TMessage message = std::move(chunkMessages[position]);
            MigrateMessage(message, *Context_->StreamSpecStorage);
            auto outputMessage = New<TOutputMessage>(std::move(message), Context_->StreamSpecStorage);
            if (!Context_->OutputStreamIds.contains(outputMessage->StreamId)) {
                THROW_ERROR_EXCEPTION("Unknown output stream %Qv during register",
                    outputMessage->StreamId);
            }
            auto [_, inserted] = MessageLocations_.emplace(
                outputMessage->MessageId,
                TMessageLocation{
                    .LocalChunkId = localChunkId,
                    .PositionInChunk = static_cast<ui64>(position),
                });
            YT_VERIFY(inserted);
            InflightStore_->Register(outputMessage);
            ++state.RemainingCount;
            messages.emplace_back(std::move(outputMessage), state.Key);
        }
        InflightStore_->SyncCounters();
        // A fully-processed chunk shouldn't survive a Sync — schedule a delayed erase if seen.
        const bool hasNoSurvivors = (state.RemainingCount == 0);
        Chunks_.emplace(localChunkId, std::move(state));
        if (hasNoSurvivors) {
            ChunksToErase_.insert(localChunkId);
        }
    }

    using TGroupKey = std::tuple<std::optional<TKey>, TStreamId>;

    // absl::flat_hash_map needs a hash adapter for tuple-of-strong-typedefs.
    struct TGroupKeyHash
    {
        size_t operator()(const TGroupKey& t) const
        {
            return THash<TGroupKey>()(t);
        }
    };

    // Groups ToPersist_ by (key, stream_id) and splits each group into chunks
    // of <=maxChunkMessageCount messages / <=MaxChunkDataSize bytes. Each chunk
    // reserves a fresh YT chunk_id from TimeProvider.
    void PackPendingMessages()
    {
        const auto maxChunkMessageCount = DynamicSpec_->MaxChunkMessageCount;
        MessageLocations_.reserve(MessageLocations_.size() + ToPersist_.size());

        const auto& specs = Context_->StreamSpecStorage->GetStreamSpecs();

        absl::flat_hash_map<TGroupKey, std::vector<TOutputMessageConstPtr>, TGroupKeyHash> groups;
        for (auto& [message, key] : ToPersist_) {
            groups[TGroupKey{key, message->StreamId}].push_back(message);
        }
        ToPersist_.clear();

        for (auto& [group, pendingMessages] : groups) {
            // Stable order for test determinism.
            auto getMessageId = [] (const auto& m) {
                return m->MessageId;
            };
            if (!IsSortedBy(pendingMessages, getMessageId)) {
                SortBy(pendingMessages, getMessageId);
            }

            const auto& [key, streamId] = group;
            i64 ytChunkId = -1;
            ui64 localChunkId = 0;
            TMessageBatchSerializer batchSerializer(specs);

            auto closeChunk = [&] {
                if (batchSerializer.IsEmpty()) {
                    return;
                }
                TChunkState state{
                    .Key = key,
                    .StreamId = streamId,
                    .ChunkId = ytChunkId,
                    .RemainingCount = batchSerializer.GetMessageCount(),
                    .SerializedBatch = batchSerializer.Finish(),
                };
                Chunks_.emplace(localChunkId, std::move(state));
                NewChunks_.insert(localChunkId);
            };

            auto openChunk = [&] {
                ytChunkId = TimeProvider_->GenerateSeqNo();
                localChunkId = NextLocalChunkId_++;
            };

            for (auto& message : pendingMessages) {
                const i64 messageByteSize = TMessageBatchSerializer::GetMessageWireSize(*message);

                const bool chunkFull = !batchSerializer.IsEmpty() &&
                    (batchSerializer.GetMessageCount() >= maxChunkMessageCount ||
                        batchSerializer.GetByteSize() + messageByteSize > MaxChunkDataSize);
                if (chunkFull) {
                    closeChunk();
                }
                if (batchSerializer.IsEmpty()) {
                    openChunk();
                }
                const int position = batchSerializer.GetMessageCount();
                batchSerializer.AddMessage(message, messageByteSize);
                auto [_, inserted] = MessageLocations_.emplace(
                    message->MessageId,
                    TMessageLocation{
                        .LocalChunkId = localChunkId,
                        .PositionInChunk = static_cast<ui64>(position),
                    });
                YT_VERIFY(inserted);
            }
            closeChunk();
        }
    }

    void ValidateSpec() const
    {
        YT_VERIFY(DynamicSpec_->MaxChunkMessageCount > 0);
        YT_VERIFY(DynamicSpec_->MaxChunkMessageCount <= MaxChunkMessageCountLimit);
    }

private:
    const TCompactOutputStoreContextPtr Context_;
    TDynamicOutputStoreSpecPtr DynamicSpec_;
    const NTables::ICompactOutputMessagesPtr KeyTable_;
    const NTables::ICompactPartitionOutputMessagesPtr PartitionTable_;
    const ITimeProviderPtr TimeProvider_;
    const NLogging::TLogger Logger;
    const TMultiInflightTrackerPtr InflightStore_;

    bool KeyStateLoaded_ = false;

    absl::flat_hash_map<TOutputMessageConstPtr, TKey, TMessageHashMapOpsByMessageId, TMessageHashMapOpsByMessageId> Keys_;

    std::deque<TMessageMeta> AsyncEraseQueue_;

    // Newly produced messages awaiting chunk packing on the next Sync.
    absl::flat_hash_map<TOutputMessageConstPtr, std::optional<TKey>, TMessageHashMapOpsByMessageId, TMessageHashMapOpsByMessageId> ToPersist_;

    struct TChunkState
    {
        // nullopt ⇒ partition chunk, otherwise keyed chunk.
        std::optional<TKey> Key;
        TStreamId StreamId;
        // YT-side chunk id (distinct from the in-memory LocalChunkId keying Chunks_).
        i64 ChunkId = 0;
        std::string ProcessedMask;
        int RemainingCount = 0;
        // Serialized message batch. Only populated while the chunk is awaiting its first
        // full-row Write (i.e. while in NewChunks_). Cleared once persisted to YT.
        TSharedRef SerializedBatch;
    };

    // LocalChunkId -> chunk state; LocalChunkId is dense and process-local.
    absl::flat_hash_map<ui64, TChunkState> Chunks_;
    ui64 NextLocalChunkId_ = 0;

    // messageId -> packed (LocalChunkId, position-in-chunk) handle; 8 bytes per entry.
    absl::flat_hash_map<TMessageId, TMessageLocation, ::THash<TMessageId>> MessageLocations_;

    // Freshly packed in PackPendingMessages, awaiting first full-row Write.
    absl::flat_hash_set<ui64> NewChunks_;

    // Persisted chunks whose processed_mask needs UpdateMask on the next Sync.
    absl::flat_hash_set<ui64> MaskDirtyChunks_;

    // Awaiting row deletion on the next Sync.
    absl::flat_hash_set<ui64> ChunksToErase_;
};

////////////////////////////////////////////////////////////////////////////////

IOutputStorePtr CreateCompactOutputStore(TCompactOutputStoreContextPtr context, TDynamicOutputStoreSpecPtr dynamicSpec)
{
    return New<TCompactOutputStore>(std::move(context), std::move(dynamicSpec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
