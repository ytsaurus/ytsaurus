#include "compact_unversioned_owning_row.h"

#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/ytree/node.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/string/string_builder.h>
#include <library/cpp/yt/yson/consumer.h>

#include <util/generic/scope.h>

namespace NYT::NFlow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

struct TCompactUnversionedOwningRowData
    : public TRefCounted
    , public TWithExtraSpace<TCompactUnversionedOwningRowData>
{
    // Extra space layout:
    //   [TUnversionedRowHeader][TUnversionedValue * Count][string data]
    // The TUnversionedRowHeader and values are at the beginning,
    // string data follows immediately after.

    size_t SpaceUsed;

    explicit TCompactUnversionedOwningRowData(size_t extraSize)
        : SpaceUsed(sizeof(TCompactUnversionedOwningRowData) + extraSize)
    { }

    TUnversionedRowHeader* GetHeader()
    {
        return static_cast<TUnversionedRowHeader*>(GetExtraSpacePtr());
    }

    const TUnversionedRowHeader* GetHeader() const
    {
        return static_cast<const TUnversionedRowHeader*>(GetExtraSpacePtr());
    }
};

DEFINE_REFCOUNTED_TYPE(TCompactUnversionedOwningRowData)

////////////////////////////////////////////////////////////////////////////////

TCompactUnversionedOwningRow::TCompactUnversionedOwningRow(const TUnversionedValueRange& range)
{
    Init(range);
}

TCompactUnversionedOwningRow::TCompactUnversionedOwningRow(const TUnversionedRow& other)
{
    if (other) {
        Init(other.Elements());
    }
}

TCompactUnversionedOwningRow::TCompactUnversionedOwningRow(const TUnversionedOwningRow& other)
{
    if (other) {
        Init(other.Elements());
    }
}

TCompactUnversionedOwningRow::operator bool() const
{
    return Data_ != nullptr;
}

TCompactUnversionedOwningRow::operator TUnversionedRow() const
{
    return Get();
}

TUnversionedRow TCompactUnversionedOwningRow::Get() const
{
    return TUnversionedRow(GetHeader());
}

const TUnversionedValue* TCompactUnversionedOwningRow::Begin() const
{
    const auto* header = GetHeader();
    return header ? reinterpret_cast<const TUnversionedValue*>(header + 1) : nullptr;
}

const TUnversionedValue* TCompactUnversionedOwningRow::End() const
{
    return Begin() + GetCount();
}

const TUnversionedValue* TCompactUnversionedOwningRow::begin() const
{
    return Begin();
}

const TUnversionedValue* TCompactUnversionedOwningRow::end() const
{
    return End();
}

TUnversionedValueRange TCompactUnversionedOwningRow::Elements() const
{
    return {Begin(), End()};
}

TUnversionedValueRange TCompactUnversionedOwningRow::FirstNElements(int count) const
{
    YT_ASSERT(count <= GetCount());
    return {Begin(), Begin() + count};
}

const TUnversionedValue& TCompactUnversionedOwningRow::operator[](int index) const
{
    YT_ASSERT(index >= 0 && index < GetCount());
    return Begin()[index];
}

int TCompactUnversionedOwningRow::GetCount() const
{
    const auto* header = GetHeader();
    return header ? static_cast<int>(header->Count) : 0;
}

size_t TCompactUnversionedOwningRow::GetSpaceUsed() const
{
    if (!Data_) {
        return 0;
    }
    return Data_->SpaceUsed;
}

TMutableUnversionedRow TCompactUnversionedOwningRow::Preallocate(int count, size_t stringDataSize)
{
    size_t rowDataSize = GetUnversionedRowByteSize(count);
    size_t extraSize = rowDataSize + stringDataSize;
    Data_ = NewWithExtraSpace<TCompactUnversionedOwningRowData>(extraSize, extraSize);

    auto* header = Data_->GetHeader();
    header->Count = count;
    header->Capacity = count;

    return TMutableUnversionedRow(header);
}

void TCompactUnversionedOwningRow::CopyStringData(int count, size_t stringDataSize)
{
    auto* header = Data_->GetHeader();
    size_t rowDataSize = GetUnversionedRowByteSize(count);
    auto* values = reinterpret_cast<TUnversionedValue*>(header + 1);
    char* stringTail = reinterpret_cast<char*>(header) + rowDataSize;
    char* current = stringTail;
    char* end = stringTail + stringDataSize;
    for (int i = 0; i < count; ++i) {
        if (IsStringLikeType(values[i].Type) && values[i].Length > 0) {
            YT_VERIFY(current + values[i].Length <= end);
            ::memcpy(current, values[i].Data.String, values[i].Length);
            values[i].Data.String = current;
            current += values[i].Length;
        }
    }
}

void TCompactUnversionedOwningRow::Init(const TUnversionedValueRange& range)
{
    int count = std::ssize(range);

    size_t stringDataSize = 0;
    for (const auto& value : range) {
        if (IsStringLikeType(value.Type)) {
            stringDataSize += value.Length;
        }
    }

    auto mutableRow = Preallocate(count, stringDataSize);
    ::memcpy(mutableRow.Begin(), range.begin(), sizeof(TUnversionedValue) * count);
    CopyStringData(count, stringDataSize);
}

const TUnversionedRowHeader* TCompactUnversionedOwningRow::GetHeader() const
{
    return Data_ ? Data_->GetHeader() : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

bool operator==(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs)
{
    return lhs.Get() == rhs.Get();
}

bool operator<(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs)
{
    return lhs.Get() < rhs.Get();
}

bool operator<=(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs)
{
    return lhs.Get() <= rhs.Get();
}

bool operator>(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs)
{
    return lhs.Get() > rhs.Get();
}

bool operator>=(const TCompactUnversionedOwningRow& lhs, const TCompactUnversionedOwningRow& rhs)
{
    return lhs.Get() >= rhs.Get();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TCompactUnversionedOwningRow& row, TStringBuf format)
{
    FormatValue(builder, row.Get(), format);
}

void Serialize(const TCompactUnversionedOwningRow& row, NYson::IYsonConsumer* consumer)
{
    Serialize(row.Get(), consumer);
}

void Deserialize(TCompactUnversionedOwningRow& row, TIntrusivePtr<NYTree::INode> node)
{
    NTableClient::TUnversionedOwningRow owningRow;
    Deserialize(owningRow, node);
    row = TCompactUnversionedOwningRow(owningRow.Elements());
}

void Deserialize(TCompactUnversionedOwningRow& row, NYson::TYsonPullParserCursor* cursor)
{
    NTableClient::TUnversionedOwningRow owningRow;
    Deserialize(owningRow, cursor);
    row = TCompactUnversionedOwningRow(owningRow.Elements());
}

void ToProto(TProtobufString* protoRow, const TCompactUnversionedOwningRow& row)
{
    NTableClient::ToProto(protoRow, row.Get());
}

struct TCompactRowFromProtoTag
{ };

void FromProto(TCompactUnversionedOwningRow* row, const TProtobufString& protoRow)
{
    NConcurrency::TForbidContextSwitchGuard contextSwitchGuard;
    static thread_local TRowBufferPtr rowBufferCache = New<TRowBuffer>(TCompactRowFromProtoTag{});
    Y_DEFER
    {
        rowBufferCache->Clear();
    };
    TUnversionedRow unversionedRow;
    NTableClient::FromProto(&unversionedRow, protoRow, rowBufferCache);
    *row = TCompactUnversionedOwningRow(unversionedRow);
}

////////////////////////////////////////////////////////////////////////////////

size_t GetWireByteSize(const TCompactUnversionedOwningRow& row)
{
    return NTableClient::GetUnversionedRowByteSizeForWire(row.Elements());
}

char* SerializeToBuffer(char* dst, const TCompactUnversionedOwningRow& row)
{
    return NTableClient::SerializeRowToBuffer(dst, row.Elements());
}

const char* DeserializeFromBuffer(const char* begin, const char* end, TCompactUnversionedOwningRow* row)
{
    ui32 count;
    const char* current = NTableClient::ReadUnversionedRowHeaderFromBuffer(begin, &count);
    // A value occupies at least one byte, so a row cannot hold more values than there are
    // remaining bytes; this bounds the allocation below against a truncated or malformed buffer.
    THROW_ERROR_EXCEPTION_UNLESS(
        current <= end && count <= static_cast<ui32>(end - current),
        "Compact unversioned row buffer is truncated or malformed");

    TCompactVector<TUnversionedValue, 16> values;
    values.resize(count);
    for (ui32 index = 0; index < count; ++index) {
        current = NTableClient::ReadUnversionedValueFromBuffer(current, &values[index]);
        THROW_ERROR_EXCEPTION_UNLESS(
            current <= end,
            "Compact unversioned row buffer is truncated or malformed");
    }

    *row = TCompactUnversionedOwningRow(TUnversionedValueRange(values.data(), values.size()));
    return current;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

////////////////////////////////////////////////////////////////////////////////

size_t THash<NYT::NFlow::TCompactUnversionedOwningRow>::operator()(
    const NYT::NFlow::TCompactUnversionedOwningRow& row) const
{
    return NYT::NTableClient::TDefaultUnversionedRowHash()(row.Get());
}
