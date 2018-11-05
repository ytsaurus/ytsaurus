#include "type_helpers.h"

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

std::vector<TString> ToString(const std::vector<std::string>& strings)
{
    std::vector<TString> result;
    result.reserve(strings.size());

    for (const auto& s: strings) {
        result.emplace_back(ToString(s));
    }

    return result;
}

std::vector<std::string> ToStdString(const std::vector<TString>& strings)
{
    std::vector<std::string> result;
    result.reserve(strings.size());

    for (const auto& s: strings) {
        result.emplace_back(ToStdString(s));
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
