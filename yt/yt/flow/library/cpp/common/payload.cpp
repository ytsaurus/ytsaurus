#include "payload.h"

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>

#include <algorithm>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TPayloadBuilder::TPayloadBuilder(NTableClient::TTableSchemaPtr schema)
    : Schema_(std::move(schema))
    , Buffer_(New<NTableClient::TRowBuffer>())
{
    Reset();
}

void TPayloadBuilder::SetValue(const NTableClient::TUnversionedValue& value)
{
    SetValue(value, value.Id);
}

void TPayloadBuilder::SetValue(const NTableClient::TUnversionedValue& value, int overrideId)
{
    SetValueImpl(value, overrideId);
}

void TPayloadBuilder::SetValue(const NTableClient::TUnversionedValue& value, TStringBuf column)
{
    int columnId = Schema_->GetColumnIndexOrThrow(column);
    SetValue(value, columnId);
}

void TPayloadBuilder::SetValueImpl(const NTableClient::TUnversionedValue& value, int overrideId, bool capture)
{
    if (overrideId >= Schema_->GetColumnCount() || overrideId < 0) {
        THROW_ERROR_EXCEPTION("Invalid column id: expected in range [%v, %v], got %v",
            0,
            Schema_->GetColumnCount() - 1,
            overrideId);
    }

    NTableClient::ValidateValueType(value, Schema_->Columns()[overrideId], /*typeAnyAcceptsAllValues*/ false, /*ignoreRequired*/ true, /*validateAnyIsValidYson*/ true);
    auto capturedValue = [&] {
        if (capture) {
            return Buffer_->CaptureValue(value);
        } else {
            return value;
        }
    }();
    capturedValue.Id = overrideId;
    if (Values_[overrideId].Type != NTableClient::EValueType::Null) {
        THROW_ERROR_EXCEPTION("Field %v already set",
            overrideId);
    }
    Values_[overrideId] = capturedValue;
}

const NTableClient::TTableSchemaPtr& TPayloadBuilder::GetSchema() const
{
    return Schema_;
}

TPayload TPayloadBuilder::Finish()
{
    auto payload = TPayload{TPayload::TUnderlying(TRange(Values_))};
    Reset();
    return payload;
}

void TPayloadBuilder::Reset()
{
    Values_.clear();
    Values_.reserve(Schema_->GetColumnCount());
    for (int i = 0; i < Schema_->GetColumnCount(); ++i) {
        Values_.push_back(NTableClient::MakeUnversionedNullValue(i));
    }
    Buffer_->Clear();
}

////////////////////////////////////////////////////////////////////////////////

bool IsEmpty(const TPayload& payload)
{
    for (const auto& value : payload.Underlying()) {
        if (value.Type != NTableClient::EValueType::Null) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TPayload& payload, int columnId)
{
    return payload.Underlying()[columnId];
}

NTableClient::TUnversionedValue GetColumn(
    const TPayload& payload,
    const NTableClient::TTableSchemaPtr& schema,
    TStringBuf columnName)
{
    YT_ASSERT(schema);
    auto columnId = schema->GetColumnIndexOrThrow(columnName);
    return GetColumn(payload, columnId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
