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

    int UpdatedInFlush = 0;
    int Reallocated = 0;

    TInstant InsertTime;
    TInstant UpdateTime;

    std::atomic<bool> Outdated = false;

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

char* CaptureStringLikeValues(NTableClient::TMutableVersionedRow versionedRow);

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
        for (const auto& value : row.Values()) {
            if (IsStringLikeType(value.Type)) {
                blobDataSize += value.Length;
            }
        }
    }

    if (!nonSentinelRow) {
        return nullptr;
    }

    int keyCount = nonSentinelRow.GetKeyCount();
    for (const auto& value : nonSentinelRow.Keys()) {
        if (IsStringLikeType(value.Type)) {
            blobDataSize += value.Length;
        }
    }

    auto rowSize = NTableClient::GetVersionedRowByteSize(
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

    auto* writeTimestampsDest = versionedRow.BeginWriteTimestamps();
    auto* deleteTimestampsDest = versionedRow.BeginDeleteTimestamps();
    auto* valuesDest = versionedRow.BeginValues();

    for (auto row : rows) {
        if (!row) {
            continue;
        }
        writeTimestampsDest = std::copy(row.BeginWriteTimestamps(), row.EndWriteTimestamps(), writeTimestampsDest);
        deleteTimestampsDest = std::copy(row.BeginDeleteTimestamps(), row.EndDeleteTimestamps(), deleteTimestampsDest);
        valuesDest = std::copy(row.BeginValues(), row.EndValues(), valuesDest);
    }

    char* blobDataDest = CaptureStringLikeValues(versionedRow);
    YT_VERIFY(blobDataDest == cachedRow->Data + rowSize + blobDataSize);

    std::sort(versionedRow.BeginWriteTimestamps(), versionedRow.EndWriteTimestamps(), std::greater<NTableClient::TTimestamp>());
    std::sort(versionedRow.BeginDeleteTimestamps(), versionedRow.EndDeleteTimestamps(), std::greater<NTableClient::TTimestamp>());

    std::sort(
        versionedRow.BeginValues(),
        versionedRow.EndValues(),
        [&] (const NTableClient::TVersionedValue& lhs, const NTableClient::TVersionedValue& rhs) {
            return lhs.Id == rhs.Id ? lhs.Timestamp > rhs.Timestamp : lhs.Id < rhs.Id;
        });

    cachedRow->Hash = NTableClient::GetFarmFingerprint(versionedRow.Keys());
    cachedRow->RetainedTimestamp = retainedTimestamp;

    return cachedRow;
}

template <class TAlloc>
TCachedRowPtr CachedRowFromVersionedRow(TAlloc* allocator, NTableClient::TVersionedRow row, NTableClient::TTimestamp retainedTimestamp)
{
    return BuildCachedRow(allocator, MakeRange(&row, 1), retainedTimestamp);
}

template <class TAlloc>
TCachedRowPtr CopyCachedRow(TAlloc* allocator, const TCachedRow* source)
{
    auto cachedRow = NewWithExtraSpace<TCachedRow>(allocator, source->Space, source->Space);
    if (!cachedRow) {
        return nullptr;
    }

    memcpy(cachedRow->Data, source->Data, source->Space);

    auto versionedRow = cachedRow->GetVersionedRow();
    CaptureStringLikeValues(versionedRow);

    cachedRow->Hash = source->Hash;
    cachedRow->RetainedTimestamp = source->RetainedTimestamp;

    cachedRow->UpdatedInFlush = source->UpdatedInFlush;
    cachedRow->Reallocated = source->Reallocated;
    cachedRow->InsertTime = source->InsertTime;
    cachedRow->UpdateTime = source->UpdateTime;
    cachedRow->Outdated.store(source->Outdated.load(std::memory_order_acquire), std::memory_order_release);

    return cachedRow;
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

    inline size_t operator()(NYT::NTableClient::TLegacyKey key) const
    {
        return GetFarmFingerprint(key.Elements());
    }

    inline size_t operator()(NYT::NTableClient::TVersionedRow row) const
    {
        return NYT::NTableClient::GetFarmFingerprint(row.Keys());
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
        return NYT::NTableClient::CompareValueRanges(lhsRow.Keys(), rhsRow.Keys()) == 0;
    }

    inline bool operator()(const NYT::NTabletNode::TCachedRow* lhs, const NYT::NTableClient::TUnversionedRow& rhs) const
    {
        auto lhsRow = lhs->GetVersionedRow();

        // Hashes are equal if Compare is called.
        return CompareValueRanges(lhsRow.Keys(), rhs.Elements()) == 0;
    }

    inline bool operator()(const NYT::NTabletNode::TCachedRow* lhs, const NYT::NTableClient::TVersionedRow& rhs) const
    {
        auto lhsRow = lhs->GetVersionedRow();

        // Hashes are equal if Compare is called.
        return NYT::NTableClient::CompareValueRanges(lhsRow.Keys(), rhs.Keys()) == 0;
    }
};
