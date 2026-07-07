#pragma once

#include <yt/yt/library/named_value/named_value.h>

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TValueExample
{
    TLogicalTypePtr LogicalType;
    NNamedValue::TNamedValue::TValue Value;
    std::string PrettyYson;

    TValueExample(TLogicalTypePtr logicalType, NNamedValue::TNamedValue::TValue value, std::string prettyYson);
};

std::vector<TValueExample> GetPrimitiveValueExamples();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
