#include "rows_digest.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <util/generic/xrange.h>

#include <contrib/libs/xxhash/xxhash.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TRowsDigestBuilder::TRowsDigestBuilder(TNameTablePtr nameTable)
    : NameTable_(std::move(nameTable))
    , HasherState_(XXH3_createState())
{
    XXH3_64bits_reset(static_cast<XXH3_state_t*>(HasherState_));

    constexpr int PreAllocateSize = 64;

    // Pre-allocate reasonable sizes.
    SortedColumns_.reserve(PreAllocateSize);
    IsColumnNameIdAdded_.reserve(PreAllocateSize);
    ValueIdToValueIndexInRow_.reserve(PreAllocateSize);
}

TRowsDigestBuilder::~TRowsDigestBuilder()
{
    XXH3_freeState(static_cast<XXH3_state_t*>(HasherState_));
}

void TRowsDigestBuilder::RegisterColumn(ui16 columnNameId) noexcept
{
    // The [[likely]]/[[unlikely]] hints provide 1-5% speed improvements. See rows_digest_bench.cpp.
    if (columnNameId >= std::ssize(IsColumnNameIdAdded_)) [[unlikely]] {
        IsColumnNameIdAdded_.resize(columnNameId + 1);
        ValueIdToValueIndexInRow_.resize_uninitialized(std::size(IsColumnNameIdAdded_));
    }

    if (IsColumnNameIdAdded_[columnNameId]) [[likely]] {
        return;
    }

    // Complexity: O(n).

    IsColumnNameIdAdded_[columnNameId] = true;

    auto name = NameTable_->GetName(columnNameId);
    ui64 nameHash = XXH3_64bits(name.data(), name.size());

    TColumnInfo newColumn(name, nameHash, columnNameId);

    auto it = std::lower_bound(
        SortedColumns_.begin(),
        SortedColumns_.end(),
        newColumn);
    SortedColumns_.insert(it, newColumn);
}

void TRowsDigestBuilder::ProcessRow(TUnversionedRow row) noexcept
{
    for (const auto& value : row) {
        RegisterColumn(value.Id);
    }

    std::ranges::fill(ValueIdToValueIndexInRow_, std::numeric_limits<ui16>::max());

    for (ui16 valueIndex : xrange(row.GetCount())) {
        ValueIdToValueIndexInRow_[row[valueIndex].Id] = valueIndex;
    }

    for (const auto& columnInfo : SortedColumns_) {
        ui16 valueIndex = ValueIdToValueIndexInRow_[columnInfo.ColumnNameId];
        if (valueIndex == std::numeric_limits<ui16>::max()) [[unlikely]] {
            continue;
        }
        AppendValue(columnInfo.NameHash, row[valueIndex]);
    }
}

void TRowsDigestBuilder::AppendValue(ui64 nameHash, TUnversionedValue value) noexcept
{
    switch (value.Type) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
        case EValueType::Boolean: {
            ui64 data = (value.Type == EValueType::Boolean)
                ? static_cast<ui64>(value.Data.Boolean)
                : value.Data.Uint64;

            AppendToHash(nameHash, value.Type, data);
            break;
        }

        case EValueType::Null:
            AppendToHash(nameHash, value.Type);
            break;

        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite: {
            AppendToHash(nameHash, value.Type);
            // String data might be large, update directly if it doesn't fit.
            if (value.Length > BatchSize / 2) {
                FlushBatch();
                XXH3_64bits_update(static_cast<XXH3_state_t*>(HasherState_), value.Data.String, value.Length);
            } else if (BatchOffset_ + value.Length > BatchSize) {
                FlushBatch();
                std::memcpy(BatchBuffer_.data() + BatchOffset_, value.Data.String, value.Length);
                BatchOffset_ += value.Length;
            } else {
                std::memcpy(BatchBuffer_.data() + BatchOffset_, value.Data.String, value.Length);
                BatchOffset_ += value.Length;
            }
            break;
        }

        default:
            YT_ABORT();
    }
}

void TRowsDigestBuilder::AppendToHash(const auto&... args) noexcept
{
    constexpr ui32 Size = (sizeof(args) + ...);

    if (BatchOffset_ + Size > BatchSize) {
        FlushBatch();
    }

    // Pack directly into batch buffer.
    ui32 offset = BatchOffset_;
    ((std::memcpy(BatchBuffer_.data() + offset, &args, sizeof(args)),
      offset += sizeof(args)), ...);

    BatchOffset_ += Size;
}

void TRowsDigestBuilder::FlushBatch() const noexcept
{
    if (BatchOffset_ > 0) {
        XXH3_64bits_update(static_cast<XXH3_state_t*>(HasherState_), BatchBuffer_.data(), BatchOffset_);
        BatchOffset_ = 0;
    }
}

TRowsDigest TRowsDigestBuilder::GetDigest() const noexcept
{
    FlushBatch();
    return TRowsDigest(XXH3_64bits_digest(static_cast<XXH3_state_t*>(HasherState_)));
}

bool TRowsDigestBuilder::TColumnInfo::operator<(const TColumnInfo& other) const noexcept
{
    return Name < other.Name;
}

TRowsDigestBuilder::TColumnInfo::TColumnInfo(TStringBuf name, ui64 nameHash, ui16 columnNameId) noexcept
    : Name(name)
    , NameHash(nameHash)
    , ColumnNameId(columnNameId)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
