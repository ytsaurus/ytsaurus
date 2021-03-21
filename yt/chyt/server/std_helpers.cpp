#include "std_helpers.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::Names ToNames(const std::vector<TString>& columnNames)
{
    DB::Names result;

    for (const auto& columnName : columnNames) {
        result.emplace_back(columnName.data());
    }

    return result;
}

std::vector<TString> ToVectorString(const DB::Names& columnNames)
{
    std::vector<TString> result;

    for (const auto& columnName : columnNames) {
        result.emplace_back(columnName);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
