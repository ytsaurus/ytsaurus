#pragma once

#include "row_helpers.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TValueExample
{
    TLogicalTypePtr LogicalType;
    TTableField::TValue Value;
    TString PrettyYson;

    TValueExample(TLogicalTypePtr logicalType, TTableField::TValue value, TString prettyYson);
};

std::vector<TValueExample> GetPrimitiveValueExamples();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
