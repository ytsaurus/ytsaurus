#include "mutable_unversioned_row.h"

#include <library/cpp/iterator/zip.h>

namespace NYT::NFlow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TMutableUnversionedOwningRow::TMutableUnversionedOwningRow(TUnversionedValueRange range)
{
    Init(range);
}

TMutableUnversionedOwningRow::TMutableUnversionedOwningRow(TUnversionedRow other)
{
    if (other) {
        Init(other.Elements());
    }
}

TMutableUnversionedOwningRow::operator bool() const
{
    return RowData_.Size() > 0;
}

TMutableUnversionedOwningRow::operator TUnversionedRow() const
{
    return Get();
}

TUnversionedRow TMutableUnversionedOwningRow::Get() const
{
    return TUnversionedRow(GetHeader());
}

const TUnversionedValue* TMutableUnversionedOwningRow::Begin() const
{
    const auto* header = GetHeader();
    return header ? reinterpret_cast<const TUnversionedValue*>(header + 1) : nullptr;
}

const TUnversionedValue* TMutableUnversionedOwningRow::End() const
{
    return Begin() + GetCount();
}

const TUnversionedValue* TMutableUnversionedOwningRow::begin() const
{
    return Begin();
}

const TUnversionedValue* TMutableUnversionedOwningRow::end() const
{
    return End();
}

TUnversionedValueRange TMutableUnversionedOwningRow::Elements() const
{
    return {Begin(), End()};
}

TUnversionedValueRange TMutableUnversionedOwningRow::FirstNElements(int count) const
{
    YT_ASSERT(count <= static_cast<int>(GetCount()));
    return {Begin(), Begin() + count};
}

const TUnversionedValue& TMutableUnversionedOwningRow::operator[](int index) const
{
    YT_ASSERT(index >= 0 && index < GetCount());
    return Begin()[index];
}

int TMutableUnversionedOwningRow::GetCount() const
{
    const auto* header = GetHeader();
    return header ? static_cast<int>(header->Count) : 0;
}

size_t TMutableUnversionedOwningRow::GetSpaceUsed() const
{
    size_t size = RowData_.Capacity();
    for (const auto& [value, holder] : Zip(Elements(), StringHolders_)) {
        if (holder) {
            size += holder->GetTotalByteSize().value_or(value.Length);
        }
    }
    return size;
}

void TMutableUnversionedOwningRow::Reserve(int count)
{
    bool wasInitialized = static_cast<bool>(*this);
    if (GetCount() >= count && wasInitialized) {
        return;
    }
    RowData_.Resize(NTableClient::GetUnversionedRowByteSize(count), /*initializeStorage*/ false);
    auto* header = GetHeader();
    if (!wasInitialized) {
        header->Count = 0;
    }
    header->Capacity = count;
    StringHolders_.reserve(count);
}

void TMutableUnversionedOwningRow::PushBack(const TUnversionedOwningValue& value)
{
    auto* header = GetHeader();
    if (!header) {
        Reserve(TUnversionedOwningRowBuilder::DefaultValueCapacity);
        header = GetHeader();
    } else if (header->Count == header->Capacity) [[unlikely]] {
        Reserve(std::max<int>(header->Capacity * 2, 1));
        // Fix header ptr after reallocation.
        header = GetHeader();
    }
    YT_ASSERT(std::ssize(StringHolders_) == header->Count);
    Data()[header->Count++] = value;
    StringHolders_.push_back(value.GetStringHolder());
}

void TMutableUnversionedOwningRow::Set(int index, const TUnversionedOwningValue& value)
{
    YT_ASSERT(index >= 0 && index < GetCount());
    Data()[index] = value;
    StringHolders_[index] = value.GetStringHolder();
}

TUnversionedOwningValue TMutableUnversionedOwningRow::GetOwning(int index) const
{
    YT_ASSERT(std::ssize(StringHolders_) == GetCount());
    YT_ASSERT(index >= 0 && index < GetCount());
    return TUnversionedOwningValue(*(Begin() + index), StringHolders_[index]);
}

TUnversionedValue* TMutableUnversionedOwningRow::Data()
{
    auto* header = GetHeader();
    return header ? reinterpret_cast<TUnversionedValue*>(header + 1) : nullptr;
}

void TMutableUnversionedOwningRow::Init(TUnversionedValueRange range)
{
    Reserve(range.size());
    for (const auto& value : range) {
        PushBack(value);
    }
}

TUnversionedRowHeader* TMutableUnversionedOwningRow::GetHeader()
{
    return static_cast<bool>(*this) ? reinterpret_cast<TUnversionedRowHeader*>(RowData_.Begin()) : nullptr;
}

const TUnversionedRowHeader* TMutableUnversionedOwningRow::GetHeader() const
{
    return static_cast<bool>(*this) ? reinterpret_cast<const TUnversionedRowHeader*>(RowData_.Begin()) : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
