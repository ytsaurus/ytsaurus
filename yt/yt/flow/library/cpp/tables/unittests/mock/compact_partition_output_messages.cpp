#include "compact_partition_output_messages.h"

namespace NYT::NFlow::NTables {

////////////////////////////////////////////////////////////////////////////////

bool TInMemoryCompactPartitionOutputMessages::TStorageKey::operator<(const TStorageKey& other) const
{
    return std::tie(PartitionId, StreamId, ChunkId) <
        std::tie(other.PartitionId, other.StreamId, other.ChunkId);
}

TFuture<std::vector<TInMemoryCompactPartitionOutputMessages::TChunk>> TInMemoryCompactPartitionOutputMessages::LoadAll(
    TFilter filter)
{
    std::vector<TChunk> result;
    for (const auto& [storageKey, value] : Storage_) {
        if (filter.PartitionId && storageKey.PartitionId != *filter.PartitionId) {
            continue;
        }
        result.push_back({
            .Key = TTableKey{
                .PartitionId = storageKey.PartitionId,
                .StreamId = storageKey.StreamId,
                .ChunkId = storageKey.ChunkId,
            },
            .Data = value.Data,
            .ProcessedMask = value.ProcessedMask,
        });
    }
    return MakeFuture(std::move(result));
}

void TInMemoryCompactPartitionOutputMessages::Write(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TChunk>& chunks,
    NCompression::ECodec /*codecId*/)
{
    for (const auto& chunk : chunks) {
        Storage_[TStorageKey{
            .PartitionId = chunk.Key.PartitionId,
            .StreamId = chunk.Key.StreamId,
            .ChunkId = chunk.Key.ChunkId,
        }] = TStorageValue{
            .Data = chunk.Data,
            .ProcessedMask = chunk.ProcessedMask,
        };
        ++WriteChunkCount_;
    }
}

void TInMemoryCompactPartitionOutputMessages::UpdateMask(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TMaskUpdate>& updates)
{
    for (const auto& update : updates) {
        auto it = Storage_.find(TStorageKey{
            .PartitionId = update.Key.PartitionId,
            .StreamId = update.Key.StreamId,
            .ChunkId = update.Key.ChunkId,
        });
        if (it != Storage_.end()) {
            it->second.ProcessedMask = update.ProcessedMask;
        }
        ++UpdateMaskCount_;
    }
}

void TInMemoryCompactPartitionOutputMessages::Erase(
    NApi::IDynamicTableTransactionPtr /*transaction*/,
    const std::vector<TTableKey>& tableKeys)
{
    for (const auto& tableKey : tableKeys) {
        Storage_.erase(TStorageKey{
            .PartitionId = tableKey.PartitionId,
            .StreamId = tableKey.StreamId,
            .ChunkId = tableKey.ChunkId,
        });
    }
}

i64 TInMemoryCompactPartitionOutputMessages::GetWriteChunkCount() const
{
    return WriteChunkCount_;
}

i64 TInMemoryCompactPartitionOutputMessages::GetUpdateMaskCount() const
{
    return UpdateMaskCount_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NTables
