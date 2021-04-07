#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/misc/slab_allocator.h>
#include <yt/yt/core/misc/atomic_ptr.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TCachedRow final
    : public TRefTracked<TCachedRow>
{
    ui64 Hash;

    // Row contains all versions of data from passive dynamic stores with flush index not greater than revision.
    std::atomic<ui32> Revision = 0;

    // Updated row constructed from current.
    // Supports case when row concurrently reinserted in lookup thread and updated in flush.
    // We cannot update Revision when reinserting existing row
    // but we have to make updated row visible from primary lookup table.
    TAtomicPtr<TCachedRow> Updated;

    NTableClient::TTimestamp RetainedTimestamp;

    const size_t Space = 0;
    char Data[0];

    explicit TCachedRow(size_t space)
        : Space(space)
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTrackerFacade::AllocateSpace(GetRefCountedTypeCookie<TCachedRow>(), Space);
#endif
    }

    ~TCachedRow()
    {
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTrackerFacade::FreeSpace(GetRefCountedTypeCookie<TCachedRow>(), Space);
#endif
    }

    using TAllocator = TSlabAllocator;
    static constexpr bool EnableHazard = true;

    NTableClient::TVersionedRow GetVersionedRow() const
    {
        return NTableClient::TVersionedRow(reinterpret_cast<const NTableClient::TVersionedRowHeader*>(&Data[0]));
    }

    NTableClient::TMutableVersionedRow GetVersionedRow()
    {
        return NTableClient::TMutableVersionedRow(reinterpret_cast<NTableClient::TVersionedRowHeader*>(&Data[0]));
    }
};

using TCachedRowPtr = TIntrusivePtr<TCachedRow>;

inline TCachedRowPtr GetLatestRow(TCachedRowPtr cachedItem)
{
    // Get last updated.
    while (cachedItem->Updated) {
        // TODO(lukyan): AcquireDangerous without hazard ptr.
        // Here weak is enough because Updated is never assigned from non null to null.
        auto updated = cachedItem->Updated.AcquireWeak();
        YT_VERIFY(updated);
        cachedItem = std::move(updated);
    }

    return cachedItem;
}

template <class TAlloc>
TCachedRowPtr BuildCachedRow(TAlloc* allocator, TRange<NTableClient::TVersionedRow> rows, NTableClient::TTimestamp retainedTimestamp)
{
    int valueCount = 0;
    int writeTimestampCount = 0;
    int deleteTimestampCount = 0;
    int blobDataSize = 0;

    NTableClient::TVersionedRow nonSentinelRow;
    for (auto row : rows) {
        if (!row) {
            continue;
        }

        nonSentinelRow = row;
        writeTimestampCount += row.GetWriteTimestampCount();
        deleteTimestampCount += row.GetDeleteTimestampCount();
        valueCount += row.GetValueCount();
        for (auto it = row.BeginValues(); it != row.EndValues(); ++it) {
            if (IsStringLikeType(it->Type)) {
                blobDataSize += it->Length;
            }
        }
    }

    if (!nonSentinelRow) {
        return nullptr;
    }

    int keyCount = nonSentinelRow.GetKeyCount();
    for (auto it = nonSentinelRow.BeginKeys(); it != nonSentinelRow.EndKeys(); ++it) {
        if (IsStringLikeType(it->Type)) {
            blobDataSize += it->Length;
        }
    }

    size_t rowSize = NTableClient::GetVersionedRowByteSize(
        keyCount,
        valueCount,
        writeTimestampCount,
        deleteTimestampCount);

    auto totalSize = rowSize + blobDataSize;
    auto cachedRow = NewWithExtraSpace<TCachedRow>(allocator, totalSize, totalSize);
    if (!cachedRow) {
        return nullptr;
    }
    auto versionedRow = cachedRow->GetVersionedRow();

    auto header = versionedRow.GetHeader();
    header->KeyCount = keyCount;
    header->ValueCount = valueCount;
    header->WriteTimestampCount = writeTimestampCount;
    header->DeleteTimestampCount = deleteTimestampCount;

    std::copy(nonSentinelRow.BeginKeys(), nonSentinelRow.EndKeys(), versionedRow.BeginKeys());

    auto writeTimestampsDest = versionedRow.BeginWriteTimestamps();
    auto deleteTimestampsDest = versionedRow.BeginDeleteTimestamps();
    auto valuesDest = versionedRow.BeginValues();

    for (auto row : rows) {
        if (!row) {
            continue;
        }
        writeTimestampsDest = std::copy(row.BeginWriteTimestamps(), row.EndWriteTimestamps(), writeTimestampsDest);
        deleteTimestampsDest = std::copy(row.BeginDeleteTimestamps(), row.EndDeleteTimestamps(), deleteTimestampsDest);
        valuesDest = std::copy(row.BeginValues(), row.EndValues(), valuesDest);
    }

    char* blobDataDest = const_cast<char*>(versionedRow.GetMemoryEnd());
    for (auto it = versionedRow.BeginKeys(); it != versionedRow.EndKeys(); ++it) {
        if (IsStringLikeType(it->Type)) {
            memcpy(blobDataDest, it->Data.String, it->Length);
            it->Data.String = blobDataDest;
            blobDataDest += it->Length;
        }
    }

    for (auto it = versionedRow.BeginValues(); it != versionedRow.EndValues(); ++it) {
        if (IsStringLikeType(it->Type)) {
            memcpy(blobDataDest, it->Data.String, it->Length);
            it->Data.String = blobDataDest;
            blobDataDest += it->Length;
        }
    }

    YT_VERIFY(blobDataDest == cachedRow->Data + rowSize + blobDataSize);

    std::sort(versionedRow.BeginWriteTimestamps(), versionedRow.EndWriteTimestamps(), std::greater<NTableClient::TTimestamp>());
    std::sort(versionedRow.BeginDeleteTimestamps(), versionedRow.EndDeleteTimestamps(), std::greater<NTableClient::TTimestamp>());

    std::sort(
        versionedRow.BeginValues(),
        versionedRow.EndValues(),
        [&] (const NTableClient::TVersionedValue& lhs, const NTableClient::TVersionedValue& rhs) {
            return lhs.Id == rhs.Id ? lhs.Timestamp > rhs.Timestamp : lhs.Id < rhs.Id;
        });

    cachedRow->Hash = GetFarmFingerprint(versionedRow.BeginKeys(), versionedRow.EndKeys());
    cachedRow->RetainedTimestamp = retainedTimestamp;

    return cachedRow;
}

template <class TAlloc>
TCachedRowPtr CachedRowFromVersionedRow(TAlloc* allocator, NTableClient::TVersionedRow row, NTableClient::TTimestamp retainedTimestamp)
{
    return BuildCachedRow(allocator, MakeRange(&row, 1), retainedTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode

template <>
struct THash<NYT::NTabletNode::TCachedRow>
{
    inline size_t operator()(const NYT::NTabletNode::TCachedRow* value) const
    {
        return value->Hash;
    }

    inline size_t operator()(const NYT::NTableClient::TUnversionedRow& key) const
    {
        return GetFarmFingerprint(key.Begin(), key.End());
    }

    inline size_t operator()(const NYT::NTableClient::TVersionedRow& key) const
    {
        return GetFarmFingerprint(key.BeginKeys(), key.EndKeys());
    }

};

template <>
struct TEqualTo<NYT::NTabletNode::TCachedRow>
{
    inline bool operator()(const NYT::NTabletNode::TCachedRow* lhs, const NYT::NTabletNode::TCachedRow* rhs) const
    {
        auto lhsRow = lhs->GetVersionedRow();
        auto rhsRow = rhs->GetVersionedRow();

        // Hashes are equal if Compare is called.
        return CompareRows(
            lhsRow.BeginKeys(),
            lhsRow.EndKeys(),
            rhsRow.BeginKeys(),
            rhsRow.EndKeys()) == 0;
    }

    inline bool operator()(const NYT::NTabletNode::TCachedRow* lhs, const NYT::NTableClient::TUnversionedRow& rhs) const
    {
        auto lhsRow = lhs->GetVersionedRow();

        // Hashes are equal if Compare is called.
        return CompareRows(
            lhsRow.BeginKeys(),
            lhsRow.EndKeys(),
            rhs.Begin(),
            rhs.End()) == 0;
    }

    inline bool operator()(const NYT::NTabletNode::TCachedRow* lhs, const NYT::NTableClient::TVersionedRow& rhs) const
    {
        auto lhsRow = lhs->GetVersionedRow();

        // Hashes are equal if Compare is called.
        return CompareRows(
            lhsRow.BeginKeys(),
            lhsRow.EndKeys(),
            rhs.BeginKeys(),
            rhs.EndKeys()) == 0;
    }
};
