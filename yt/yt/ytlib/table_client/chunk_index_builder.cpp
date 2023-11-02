#include "chunk_index_builder.h"

#include "chunk_index.h"
#include "config.h"
#include "versioned_block_writer.h"

#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/core/profiling/timing.h>

#include <util/random/random.h>

namespace NYT::NTableClient {

using NYT::ToProto;

using namespace NChunkClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

struct THashTableChunkIndexBlobTag
{ };

class THashTableChunkIndexBuilder
    : public IChunkIndexBuilder
{
public:
    THashTableChunkIndexBuilder(
        THashTableChunkIndexWriterConfigPtr config,
        const TIndexedVersionedBlockFormatDetail& blockFormatDetail,
        const NLogging::TLogger& logger)
        : Config_(std::move(config))
        , BlockFormatDetail_(blockFormatDetail)
        , Logger(logger)
    {
        if (Config_->MaxBlockSize) {
            if (*Config_->MaxBlockSize < THashTableChunkIndexFormatDetail::SectorSize) {
                THROW_ERROR_EXCEPTION("Cannot build hash table chunk index for specified parameters")
                    << TErrorAttribute("max_block_size", *Config_->MaxBlockSize)
                    << TErrorAttribute("sector_size", THashTableChunkIndexFormatDetail::SectorSize);
            }

            auto maxSlotCountInBlock = THashTableChunkIndexFormatDetail::GetMaxSlotCountInBlock(
                BlockFormatDetail_.GetGroupCount(),
                Config_->EnableGroupReordering,
                *Config_->MaxBlockSize);
            MaxEntryCountInBlock_ = maxSlotCountInBlock * Config_->LoadFactor;

            if (MaxEntryCountInBlock_ == 0) {
                THROW_ERROR_EXCEPTION("Cannot build hash table chunk index for specified parameters")
                    << TErrorAttribute("max_block_size", *Config_->MaxBlockSize)
                    << TErrorAttribute("load_factor", Config_->LoadFactor);
            }
        }
    }

    void ProcessRow(TVersionedRow row, TChunkIndexEntry entry) override
    {
        Entries_.emplace_back(row, std::move(entry));
        if (std::ssize(Entries_) % MaxEntryCountInBlock_ == 0) {
            BlockLastKeys_.emplace_back(row.Keys());
        }
    }

    std::vector<TSharedRef> BuildIndex(
        TUnversionedValueRange lastKey,
        TSystemBlockMetaExt* systemBlockMetaExt) override
    {
        return DoBuildIndex(lastKey, systemBlockMetaExt, /*startSlotIndexes*/ {});
    }

    std::vector<TSharedRef> BuildIndex(
        TUnversionedValueRange lastKey,
        NProto::TSystemBlockMetaExt* systemBlockMetaExt,
        const std::vector<int>& rowToSlotIndex) override
    {
        YT_VERIFY(Entries_.size() == rowToSlotIndex.size());

        return DoBuildIndex(lastKey, systemBlockMetaExt, rowToSlotIndex);
    }

    EBlockType GetBlockType() const override
    {
        return EBlockType::HashTableChunkIndex;
    }

private:
    struct TChunkIndexBlock
    {
        TSharedRef Data;
        TSystemBlockMeta Meta;
    };

    struct THashTableChunkIndexEntry
        : public TChunkIndexEntry
    {
        THashTableChunkIndexEntry(TVersionedRow row, TChunkIndexEntry entry)
            : TChunkIndexEntry(std::move(entry))
            , Fingerprint(GetFarmFingerprint(row.Keys()))
        { }

        const TFingerprint Fingerprint;
    };

    const THashTableChunkIndexWriterConfigPtr Config_;
    // NB: Chunk writer holds this object.
    const TIndexedVersionedBlockFormatDetail& BlockFormatDetail_;
    const NLogging::TLogger Logger;

    int MaxEntryCountInBlock_ = std::numeric_limits<int>::max();

    std::vector<THashTableChunkIndexEntry> Entries_;
    std::vector<TLegacyOwningKey> BlockLastKeys_;


    std::vector<TSharedRef> DoBuildIndex(
        TUnversionedValueRange lastKey,
        TSystemBlockMetaExt* systemBlockMetaExt,
        TRange<int> startSlotIndexes)
    {
        YT_VERIFY(!Entries_.empty());

        NProfiling::TWallTimer timer;

        if (std::ssize(Entries_) % MaxEntryCountInBlock_ != 0) {
            BlockLastKeys_.emplace_back(lastKey);
        }

        std::vector<TSharedRef> blocks;
        blocks.reserve(BlockLastKeys_.size());

        int entryIndex = 0;
        for (auto blockLastKey : BlockLastKeys_) {
            auto nextEntryIndex = std::min(entryIndex + MaxEntryCountInBlock_, static_cast<int>(Entries_.size()));
            YT_VERIFY(entryIndex != nextEntryIndex);

            // May be non-empty for testing purposes.
            auto startSlotIndexesSlice = startSlotIndexes
                ? startSlotIndexes.Slice(entryIndex, nextEntryIndex)
                : startSlotIndexes;

            auto chunkIndexBlock = BuildChunkIndexBlock(
                MakeRange(
                    Entries_.begin() + entryIndex,
                    Entries_.begin() + nextEntryIndex),
                startSlotIndexesSlice,
                blockLastKey);

            blocks.push_back(std::move(chunkIndexBlock.Data));
            systemBlockMetaExt->add_system_blocks()->Swap(&chunkIndexBlock.Meta);

            entryIndex = nextEntryIndex;
        }

        YT_VERIFY(entryIndex == std::ssize(Entries_));

        YT_LOG_DEBUG("Hash table chunk index is built "
            "(BlockCount: %v, EntryCount: %v, Size: %v, WallTime: %v)",
            blocks.size(),
            Entries_.size(),
            GetByteSize(blocks),
            timer.GetElapsedTime());

        return blocks;
    }

    TChunkIndexBlock BuildChunkIndexBlock(
        TRange<THashTableChunkIndexEntry> entries,
        TRange<int> startSlotIndexes,
        const TLegacyOwningKey& blockLastKey)
    {
        auto slotCount = std::ssize(entries) / Config_->LoadFactor;

        std::optional<int> bestPenalty;
        std::vector<std::optional<int>> bestSlotIndexToEntryIndex;
        std::optional<THashTableChunkIndexFormatDetail> bestFormatDetail;

        std::vector<std::optional<int>> slotIndexToEntryIndex;

        static constexpr int TypicalEntryCountPerSlot = 1;
        std::vector<TCompactVector<int, TypicalEntryCountPerSlot>> slotIndexToCandidateIndexes;

        for (int rehashIteration = 0; rehashIteration < Config_->RehashTrialCount; ++rehashIteration) {
            THashTableChunkIndexFormatDetail formatDetail(
                /*seed*/ RandomNumber<ui64>(),
                slotCount,
                BlockFormatDetail_.GetGroupCount(),
                Config_->EnableGroupReordering);

            slotIndexToCandidateIndexes.resize(slotCount);
            for (int entryIndex = 0; entryIndex < std::ssize(entries); ++entryIndex) {
                auto slotIndex = startSlotIndexes
                    ? startSlotIndexes[entryIndex]
                    : formatDetail.GetStartSlotIndex(entries[entryIndex].Fingerprint);
                slotIndexToCandidateIndexes[slotIndex].push_back(entryIndex);
            }

            int penalty = 0;
            std::queue<std::pair<int, int>> pendingCandidates;
            slotIndexToEntryIndex.resize(slotCount);
            for (int slotIndex = 0; slotIndex < slotCount; ++slotIndex) {
                auto& candidateIndexes = slotIndexToCandidateIndexes[slotIndex];
                auto candidatesIt = candidateIndexes.begin();

                if (!pendingCandidates.empty()) {
                    auto [entryIndex, startSlotIndex] = pendingCandidates.front();
                    pendingCandidates.pop();
                    slotIndexToEntryIndex[slotIndex] = entryIndex;

                    YT_VERIFY(slotIndex > startSlotIndex);
                    penalty += slotIndex - startSlotIndex;
                } else if (candidatesIt != candidateIndexes.end()) {
                    slotIndexToEntryIndex[slotIndex] = *candidatesIt;
                    ++candidatesIt;
                } else {
                    slotIndexToEntryIndex[slotIndex] = std::nullopt;
                }

                while (candidatesIt != candidateIndexes.end()) {
                    pendingCandidates.emplace(*candidatesIt, slotIndex);
                    ++candidatesIt;
                }

                candidateIndexes.clear();
            }

            // Assign slots for candidate entries left after the first iteration over the slots.
            int slotIndex = 0;
            while (!pendingCandidates.empty()) {
                YT_VERIFY(slotIndex < std::ssize(slotIndexToEntryIndex));
                if (!slotIndexToEntryIndex[slotIndex]) {
                    auto [entryIndex, startSlotIndex] = pendingCandidates.front();
                    pendingCandidates.pop();
                    slotIndexToEntryIndex[slotIndex] = entryIndex;

                    YT_VERIFY(slotIndex < startSlotIndex);
                    penalty += slotCount + slotIndex - startSlotIndex;
                }
                ++slotIndex;
            }

            if (!bestPenalty || *bestPenalty > penalty) {
                bestPenalty = penalty;
                std::swap(bestSlotIndexToEntryIndex, slotIndexToEntryIndex);
                bestFormatDetail.emplace(formatDetail);
            }
        }

        YT_VERIFY(bestPenalty);

        auto chunkIndexSize = bestFormatDetail->GetChunkIndexByteSize();
        YT_VERIFY(Config_->MaxBlockSize.value_or(std::numeric_limits<i64>::max()) >= chunkIndexSize);
        auto blob = TSharedMutableRef::Allocate<THashTableChunkIndexBlobTag>(
            chunkIndexSize,
            { .InitializeStorage = false });

        int slotIndex = 0;
        auto* buffer = blob.Begin();
        for (int sectorIndex = 0; sectorIndex < bestFormatDetail->GetSectorCount(); ++sectorIndex) {
            auto* sectorStart = buffer;
            int startSlotIndex = slotIndex;
            while (slotIndex < startSlotIndex + bestFormatDetail->GetSlotCountInSector()) {
                if (auto entryIndex = bestSlotIndexToEntryIndex[slotIndex]) {
                    SerializeEntry(buffer, entries[*entryIndex], *bestFormatDetail);
                } else {
                    SerializeEmptyEntry(buffer, *bestFormatDetail);
                }

                ++slotIndex;
                if (slotIndex == slotCount) {
                    break;
                }
            }

            i64 paddingSize =
                THashTableChunkIndexFormatDetail::SectorSize - (buffer - sectorStart) - sizeof(TChecksum);
            YT_VERIFY(paddingSize >= 0);
            WriteZeroes(buffer, paddingSize);

            WriteChecksum(buffer, buffer - sectorStart);
        }

        YT_VERIFY(buffer - blob.Begin() == bestFormatDetail->GetChunkIndexByteSize());

        TSystemBlockMeta meta;
        auto* chunkIndexMetaExt = meta.MutableExtension(
            THashTableChunkIndexSystemBlockMeta::hash_table_chunk_index_system_block_meta_ext);
        chunkIndexMetaExt->set_seed(bestFormatDetail->GetSeed());
        chunkIndexMetaExt->set_slot_count(slotCount);
        ToProto(
            chunkIndexMetaExt->mutable_last_key(),
            blockLastKey);

        return {
            .Data = std::move(blob),
            .Meta = std::move(meta)
        };
    }

    void SerializeEntry(
        char*& buffer,
        const THashTableChunkIndexEntry& entry,
        const THashTableChunkIndexFormatDetail& formatDetail)
    {
        auto* bufferStart = buffer;

        WritePod(buffer, formatDetail.GetSerializableFingerprint(entry.Fingerprint));
        WritePod(buffer, entry.BlockIndex);
        WritePod(buffer, entry.RowOffset);
        WritePod(buffer, entry.RowLength);
        for (auto groupOffset : entry.GroupOffsets) {
            WritePod(buffer, groupOffset);
        }
        for (auto groupIndex : entry.GroupIndexes) {
            WritePod(buffer, groupIndex);
        }

        YT_ASSERT(buffer - bufferStart == formatDetail.GetEntryByteSize());
    }

    void SerializeEmptyEntry(
        char*& buffer,
        const THashTableChunkIndexFormatDetail& formatDetail)
    {
        auto* bufferStart = buffer;
        WritePod(buffer, formatDetail.MissingEntryFingerprint);
        WriteZeroes(
            buffer,
            formatDetail.GetEntryByteSize() - sizeof(formatDetail.MissingEntryFingerprint));
        YT_ASSERT(buffer - bufferStart == formatDetail.GetEntryByteSize());
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkIndexBuilderPtr CreateChunkIndexBuilder(
    const TChunkIndexesWriterConfigPtr& config,
    const TIndexedVersionedBlockFormatDetail& blockFormatDetail,
    const NLogging::TLogger& logger)
{
    return New<THashTableChunkIndexBuilder>(
        config->HashTable,
        blockFormatDetail,
        logger);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
