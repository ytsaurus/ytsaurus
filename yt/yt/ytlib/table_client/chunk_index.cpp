#include "chunk_index.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/collection_helpers.h>
#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/numeric_helpers.h>

#include <library/cpp/yt/small_containers/compact_flat_map.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(akozhikhov): Write checksum ahead of the blob as in hunk checksums?
void WriteChecksum(char*& buffer, i64 byteSize)
{
    WritePod(buffer, GetChecksum(TRef(buffer - byteSize, byteSize)));
}

////////////////////////////////////////////////////////////////////////////////

TIndexedVersionedBlockFormatDetail::TIndexedVersionedBlockFormatDetail(
    const TTableSchemaPtr& schema)
    : KeyColumnCount_(schema->GetKeyColumnCount())
{
    TCompactFlatMap<std::optional<TString>, int, IndexedRowTypicalGroupCount> groupNameToIndex;
    TCompactVector<int, IndexedRowTypicalGroupCount> groupColumnCounts;

    ColumnIdToColumnInfo_.reserve(schema->GetValueColumnCount());

    for (int index = schema->GetKeyColumnCount(); index < schema->GetColumnCount(); ++index) {
        const auto& column = schema->Columns()[index];

        auto it = groupNameToIndex.find(column.Group());
        if (it == groupNameToIndex.end()) {
            it = EmplaceOrCrash(
                groupNameToIndex,
                column.Group(),
                static_cast<int>(groupNameToIndex.size()));

            groupColumnCounts.push_back(0);
        }

        ColumnIdToColumnInfo_.push_back({
            .GroupIndex = it->second,
            .ColumnIndexInGroup = groupColumnCounts[it->second],
        });

        ++groupColumnCounts[it->second];
    }

    for (auto& columnInfo : ColumnIdToColumnInfo_) {
        columnInfo.ColumnCountInGroup = groupColumnCounts[columnInfo.GroupIndex];
    }

    GroupCount_ = std::ssize(groupNameToIndex);
}

TIndexedVersionedBlockFormatDetail::TColumnInfo
TIndexedVersionedBlockFormatDetail::GetValueColumnInfo(int valueId) const
{
    YT_ASSERT(valueId >= KeyColumnCount_);
    return ColumnIdToColumnInfo_[valueId - KeyColumnCount_];
}

int TIndexedVersionedBlockFormatDetail::GetGroupCount() const
{
    return GroupCount_;
}

std::vector<int> TIndexedVersionedBlockFormatDetail::GetGroupIndexesToRead(
    const std::vector<TColumnIdMapping>& schemaIdMapping) const
{
    if (GroupCount_ == 1) {
        return {};
    }

    std::vector<int> groupIndicesToRead;
    for (const auto& columnIdMapping : schemaIdMapping) {
        groupIndicesToRead.push_back(GetValueColumnInfo(columnIdMapping.ChunkSchemaIndex).GroupIndex);
    }

    SortUnique(groupIndicesToRead);
    if (GroupCount_ == std::ssize(groupIndicesToRead)) {
        return {};
    }

    return groupIndicesToRead;
}

////////////////////////////////////////////////////////////////////////////////

THashTableChunkIndexFormatDetail::THashTableChunkIndexFormatDetail(
    ui64 seed,
    int slotCount,
    int groupCount,
    bool groupReorderingEnabled)
    : Seed_(seed)
    , SlotCount_(slotCount)
{
    EntryByteSize_ = GetEntryByteSize(groupCount, groupReorderingEnabled);

    if (SectorDataSize < EntryByteSize_) {
        THROW_ERROR_EXCEPTION("Cannot build hash table chunk index for specified format parameters")
            << TErrorAttribute("entry_byte_size", EntryByteSize_)
            << TErrorAttribute("sector_data_size", SectorDataSize);
    }

    SlotCountInSector_ = SectorDataSize / EntryByteSize_;

    SectorCount_ = DivCeil(SlotCount_, SlotCountInSector_);

    ChunkIndexByteSize_ = SectorCount_ * SectorSize;
}

ui64 THashTableChunkIndexFormatDetail::GetSeed() const
{
    return Seed_;
}

int THashTableChunkIndexFormatDetail::GetStartSlotIndex(TFingerprint fingerprint) const
{
    return FarmFingerprint(fingerprint, Seed_) % SlotCount_;
}

int THashTableChunkIndexFormatDetail::GetNextSlotIndex(int slotIndex) const
{
    YT_VERIFY(slotIndex < SlotCount_);
    return slotIndex + 1 == SlotCount_ ? 0 : slotIndex + 1;
}

int THashTableChunkIndexFormatDetail::GetSectorIndex(int slotIndex) const
{
    YT_VERIFY(slotIndex < SlotCount_);
    return slotIndex / SlotCountInSector_;
}

THashTableChunkIndexFormatDetail::TSerializableFingerprint
THashTableChunkIndexFormatDetail::GetSerializableFingerprint(TFingerprint fingerprint) const
{
    TSerializableFingerprint serializableFingerprint = fingerprint;
    return serializableFingerprint == MissingEntryFingerprint
        ? MinPresentEntryFingerprint
        : serializableFingerprint;
}

bool THashTableChunkIndexFormatDetail::IsEntryPresent(TSerializableFingerprint fingerprint) const
{
    return fingerprint != MissingEntryFingerprint;
}

THashTableChunkIndexFormatDetail::TSerializableFingerprint
THashTableChunkIndexFormatDetail::NarrowFingerprint(
    TSerializableFingerprint fingerprint,
    int fingerprintDomainSize) const
{
    YT_VERIFY(fingerprintDomainSize > 1);

    if (fingerprint == MissingEntryFingerprint) {
        return fingerprint;
    }

    fingerprint %= fingerprintDomainSize;
    return fingerprint == MissingEntryFingerprint
        ? MinPresentEntryFingerprint
        : fingerprint;
}

int THashTableChunkIndexFormatDetail::GetMaxSlotCountInBlock(
    int groupCount,
    bool groupReorderingEnabled,
    int maxBlockSize)
{
    auto entryByteSize = GetEntryByteSize(groupCount, groupReorderingEnabled);
    auto slotCountInSector = SectorDataSize / entryByteSize;
    return (maxBlockSize / SectorSize) * slotCountInSector;
}

int THashTableChunkIndexFormatDetail::GetEntryByteSize(
    int groupCount,
    bool groupReorderingEnabled)
{
    int entryByteSize = sizeof(TSerializableFingerprint) + sizeof(i32) + 2 * sizeof(i64);
    if (groupCount != 1) {
        entryByteSize += groupCount * sizeof(i32);
        entryByteSize += (groupReorderingEnabled ? groupCount * sizeof(i32) : 0);
    }

    return entryByteSize;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
