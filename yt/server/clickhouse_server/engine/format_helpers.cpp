#include "format_helpers.h"

#include "type_helpers.h"

#include <util/stream/output.h>
#include <util/string/join.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

std::string Quoted(const TString& name)
{
    return ToStdString(name.Quote());
}

std::string Quoted(const std::string& name)
{
    return ToStdString(ToString(name).Quote());
}

std::string JoinStrings(const TString& delimiter, const std::vector<TString>& strings)
{
    return ToStdString(::JoinRange(delimiter, strings.begin(), strings.end()));
}

} // namespace NClickHouse
} // namespace NYT
