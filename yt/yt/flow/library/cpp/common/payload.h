#pragma once

#include "public.h"

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

class TPayloadBuilder
{
public:
    explicit TPayloadBuilder(NTableClient::TTableSchemaPtr schema);

    //! Uses value.Id to determine related column from schema.
    //! Performs value validation.

    template <class T>
    void Set(T value, int columnId)
    {
        NTableClient::TUnversionedValue unversionedValue{};
        NTableClient::ToUnversionedValue<T>(&unversionedValue, value, Buffer_);
        SetValueImpl(unversionedValue, columnId, false);
    }

    template <class T>
    void Set(T value, TStringBuf column)
    {
        int columnId = Schema_->GetColumnIndexOrThrow(column);
        NTableClient::TUnversionedValue unversionedValue{};
        NTableClient::ToUnversionedValue<T>(&unversionedValue, value, Buffer_);
        SetValueImpl(unversionedValue, columnId, false);
    }

    void SetValue(const NTableClient::TUnversionedValue& value);
    void SetValue(const NTableClient::TUnversionedValue& value, int overrideId);
    // Inefficient
    void SetValue(const NTableClient::TUnversionedValue& value, TStringBuf column);

    const NTableClient::TTableSchemaPtr& GetSchema() const;

    //! Returns unversioned row where all schema's columns are filled.
    //! And value.Id == value_index in result.
    TPayload Finish();
    void Reset();

private:
    const NTableClient::TTableSchemaPtr Schema_;
    const NTableClient::TRowBufferPtr Buffer_;
    std::vector<NTableClient::TUnversionedValue> Values_;

    void SetValueImpl(const NTableClient::TUnversionedValue& value, int overrideId, bool capture = true);
};

////////////////////////////////////////////////////////////////////////////////

bool IsEmpty(const TPayload& payload);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TPayload& payload, int columnId);

// Inefficient.
NTableClient::TUnversionedValue GetColumn(
    const TPayload& payload,
    const NTableClient::TTableSchemaPtr& schema,
    TStringBuf columnName);

template <class T>
T GetColumnValue(const TPayload& payload, int columnId);

// Inefficient.
template <class T>
T GetColumnValue(
    const TPayload& payload,
    const NTableClient::TTableSchemaPtr& schema,
    TStringBuf columnName);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define PAYLOAD_INL_H_
#include "payload-inl.h"
#undef PAYLOAD_INL_H_
