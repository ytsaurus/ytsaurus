#pragma once

#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/versioned_row.h>

#include <yt/core/misc/slab_allocator.h>
#include <yt/core/misc/atomic_ptr.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct TCachedRow final
{
    ui64 Hash;
    char Data[0];

    using TAllocator = TSlabAllocator;
    using TEnableHazard = void;

    NTableClient::TVersionedRow GetVersionedRow() const
    {
        return NTableClient::TVersionedRow(reinterpret_cast<const NTableClient::TVersionedRowHeader*>(&Data[0]));
    }

    NTableClient::TMutableVersionedRow GetVersionedRow()
    {
        return NTableClient::TMutableVersionedRow(reinterpret_cast<NTableClient::TVersionedRowHeader*>(&Data[0]));
    }
};

using TCachedRowPtr = TRefCountedPtr<TCachedRow>;

template <class TAlloc>
TCachedRowPtr CachedRowFromVersionedRow(TAlloc* allocator, NTableClient::TVersionedRow row)
{
    int rowSize = row.GetMemoryEnd() - row.GetMemoryBegin();

    int stringDataSize = 0;
    for (auto it = row.BeginKeys(); it != row.EndKeys(); ++it) {
        if (IsStringLikeType(it->Type)) {
            stringDataSize += it->Length;
        }
    }

    for (auto it = row.BeginValues(); it != row.EndValues(); ++it) {
        if (IsStringLikeType(it->Type)) {
            stringDataSize += it->Length;
        }
    }

    auto cachedRow = NewWithExtraSpace<TCachedRow>(allocator, rowSize + stringDataSize);

    memcpy(&cachedRow->Data[0], row.GetMemoryBegin(), rowSize);

    auto capturedRow = cachedRow->GetVersionedRow();
    char* dest = const_cast<char*>(capturedRow.GetMemoryEnd());

    for (auto it = capturedRow.BeginKeys(); it != capturedRow.EndKeys(); ++it) {
        if (IsStringLikeType(it->Type)) {
            memcpy(dest, it->Data.String, it->Length);
            it->Data.String = dest;
            dest += it->Length;
        }
    }

    for (auto it = capturedRow.BeginValues(); it != capturedRow.EndValues(); ++it) {
        if (IsStringLikeType(it->Type)) {
            memcpy(dest, it->Data.String, it->Length);
            it->Data.String = dest;
            dest += it->Length;
        }
    }

    cachedRow->Hash = GetFarmFingerprint(row.BeginKeys(), row.EndKeys());

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
