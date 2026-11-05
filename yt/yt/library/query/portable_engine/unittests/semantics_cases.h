#pragma once

#include <yt/yt/library/query/base/query_common.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <string>
#include <vector>

namespace NYT::NQueryClient::NPortable::NTest {

////////////////////////////////////////////////////////////////////////////////

struct TScalarExpressionCase
{
    std::string Name;
    std::string Source;
    NTableClient::TTableSchema Schema;
    NTableClient::TUnversionedOwningRow InputRow;
    TOwningValue ExpectedValue;
    std::string ExpectedError;
    int BuilderVersion = 1;
    std::vector<std::string> Capabilities;
};

const std::vector<TScalarExpressionCase>& GetScalarExpressionCases();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NPortable::NTest
