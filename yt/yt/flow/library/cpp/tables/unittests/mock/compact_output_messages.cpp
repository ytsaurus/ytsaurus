#include "compact_output_messages.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryCompactOutputMessages::TStorageKey::operator<(const TStorageKey& other) const
{
    return std::tie(ComputationId, Key, StreamId, ChunkId) <
        std::tie(other.ComputationId, other.Key, other.StreamId, other.ChunkId);
}

TFuture<std::vector<TInMemoryCompactOutputMessages::TChunk>> TInMemoryCompactOutputMessages::LoadAll(
    TFilter filter)
{
    std::vector<TChunk> result;
    for (const auto& [storageKey, value] : Storage_) {
        if (filter.ComputationId && storageKey.ComputationId != *filter.ComputationId) {
            continue;
        }
        if (filter.ExactKey && storageKey.Key != *filter.ExactKey) {
            continue;
        }
        if (filter.LowerKey && storageKey.Key < *filter.LowerKey) {
            continue;
        }
        if (filter.UpperKey && !(storageKey.Key < *filter.UpperKey)) {
            continue;
        }
        result.push_back({
            .Key = TTableKey{
                .ComputationId = storageKey.ComputationId,
                .Key = storageKey.Key,
                .StreamId = storageKey.StreamId,
                .ChunkId = storageKey.ChunkId,
            },
            .Data = value.Data,
            .ProcessedMask = value.ProcessedMask,
        });
    }
    return MakeFuture(std::move(result));
}

void TInMemoryCompactOutputMessages::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TChunk>& chunks,
    NCompression::ECodec /*codecId*/)
{
    for (const auto& chunk : chunks) {
        Storage_[TStorageKey{
            .ComputationId = chunk.Key.ComputationId,
            .Key = chunk.Key.Key,
            .StreamId = chunk.Key.StreamId,
            .ChunkId = chunk.Key.ChunkId,
        }] = TStorageValue{
            .Data = chunk.Data,
            .ProcessedMask = chunk.ProcessedMask,
        };
        ++WriteChunkCount_;
    }
}

void TInMemoryCompactOutputMessages::UpdateMask(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TMaskUpdate>& updates)
{
    for (const auto& update : updates) {
        auto it = Storage_.find(TStorageKey{
            .ComputationId = update.Key.ComputationId,
            .Key = update.Key.Key,
            .StreamId = update.Key.StreamId,
            .ChunkId = update.Key.ChunkId,
        });
        if (it != Storage_.end()) {
            it->second.ProcessedMask = update.ProcessedMask;
        }
        ++UpdateMaskCount_;
    }
}

void TInMemoryCompactOutputMessages::Erase(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TTableKey>& tableKeys)
{
    for (const auto& tableKey : tableKeys) {
        Storage_.erase(TStorageKey{
            .ComputationId = tableKey.ComputationId,
            .Key = tableKey.Key,
            .StreamId = tableKey.StreamId,
            .ChunkId = tableKey.ChunkId,
        });
    }
}

i64 TInMemoryCompactOutputMessages::GetWriteChunkCount() const
{
    return WriteChunkCount_;
}

i64 TInMemoryCompactOutputMessages::GetUpdateMaskCount() const
{
    return UpdateMaskCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
