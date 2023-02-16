#pragma once

#include "public.h"

#include <yt/yt/core/misc/property.h>
#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

void WriteChecksum(char*& buffer, i64 byteSize);

////////////////////////////////////////////////////////////////////////////////

//! Determines some properties of indexed versioned block format.
//! Namely, order of groups in a row and order of columns in a group.
class TIndexedVersionedBlockFormatDetail
{
public:
    explicit TIndexedVersionedBlockFormatDetail(const TTableSchemaPtr& schema);

    struct TColumnInfo
    {
        int GroupIndex;
        int ColumnCountInGroup;
        int ColumnIndexInGroup;
    };

    TColumnInfo GetValueColumnInfo(int valueId) const;

    int GetGroupCount() const;

    //! Returns column groups subset that should be read according to #schemaIdMapping.
    //! Empty vector is returned in case all the groups are to be read.
    std::vector<int> GetGroupIndexesToRead(
        const std::vector<TColumnIdMapping>& schemaIdMapping) const;

private:
    const int KeyColumnCount_;

    int GroupCount_;
    std::vector<TColumnInfo> ColumnIdToColumnInfo_;
};

////////////////////////////////////////////////////////////////////////////////

//! Determines some properties of hash table chunk index format.
/*!
 *  Hash table chunk index layout relies on these format details.
 *
 *  It consists of sectors, namely 4_KB blobs.
 *  Each sector consists of multiple chunk index slots and checksum:
 *      | slot_1 | ... | slot_n | sector checksum |
 *  Each slot corresponds to either null or actual entry:
 *      1) | MissingEntryFingerprint | 0 ... 0 |
 *          NB: Zero padding to match actual entry byte size.
 *      2) | EntryFingerprint | block_index | offset_in_block | row_length | group_offsets | group_indexes |
 *          NB: 'group_offsets' are present iff GroupCount > 1.
 *              'group_indexes' are present iff GroupCount > 1 and group reordering is enabled.
 */

class THashTableChunkIndexFormatDetail
{
public:
    using TSerializableFingerprint = ui32;

    static constexpr i64 SectorSize = 4_KB;
    static constexpr TSerializableFingerprint MissingEntryFingerprint = 0u;

    DEFINE_BYVAL_RO_PROPERTY(int, EntryByteSize);
    DEFINE_BYVAL_RO_PROPERTY(int, SlotCountInSector);
    DEFINE_BYVAL_RO_PROPERTY(int, SectorCount);
    DEFINE_BYVAL_RO_PROPERTY(i64, ChunkIndexByteSize);

    THashTableChunkIndexFormatDetail(
        ui64 seed,
        int slotCount,
        int groupCount,
        bool groupReorderingEnabled);

    THashTableChunkIndexFormatDetail(const THashTableChunkIndexFormatDetail& other) = default;

    ui64 GetSeed() const;

    int GetStartSlotIndex(TFingerprint fingerprint) const;
    int GetNextSlotIndex(int slotIndex) const;

    int GetSectorIndex(int slotIndex) const;

    TSerializableFingerprint GetSerializableFingerprint(TFingerprint fingerprint) const;
    bool IsEntryPresent(TSerializableFingerprint fingerprint) const;
    //! For testing purposes.
    TSerializableFingerprint NarrowFingerprint(TSerializableFingerprint fingerprint, int fingerprintDomainSize) const;

    static int GetMaxSlotCountInBlock(int groupCount, bool groupReorderingEnabled, int maxBlockSize);

private:
    static constexpr i64 SectorDataSize = SectorSize - sizeof(TChecksum);
    static constexpr TSerializableFingerprint MinPresentEntryFingerprint = 1;
    static_assert(MinPresentEntryFingerprint != MissingEntryFingerprint);

    const ui64 Seed_;
    const int SlotCount_;


    static int GetEntryByteSize(int groupCount, bool groupReorderingEnabled);
};

static_assert(THashTableChunkIndexFormatDetail::SectorSize % SerializationAlignment == 0);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
