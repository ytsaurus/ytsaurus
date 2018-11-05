#pragma once

#include <util/generic/string.h>

#include <string>
#include <vector>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

std::string Quoted(const TString& name);
std::string Quoted(const std::string& name);

std::string JoinStrings(const TString& delimiter, const std::vector<TString>& strings);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
