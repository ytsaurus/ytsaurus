#pragma once

#ifndef PAYLOAD_INL_H_
    #error "Direct inclusion of this file is not allowed, include payload.h"
    // For sane code completion.
    #include "payload.h"
#endif

#include <yt/yt/client/table_client/helpers.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GetColumnValue(const TPayload& payload, int columnId)
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(payload, columnId));
}

template <class T>
T GetColumnValue(
    const TPayload& payload,
    const NTableClient::TTableSchemaPtr& schema,
    TStringBuf columnName)
{
    return NTableClient::FromUnversionedValue<T>(GetColumn(payload, schema, columnName));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
