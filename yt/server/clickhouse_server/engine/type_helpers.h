#pragma once

#include "clickhouse.h"

#include <yt/core/misc/public.h>

//#include <Poco/Timestamp.h>

#include <string>
#include <vector>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

inline TString ToString(const std::string& s)
{
    return { s.data(), s.size() };
}

inline std::string ToStdString(const TString& s)
{
    return { s.data(), s.size() };
}

inline std::string ToStdString(TStringBuf s)
{
    return { s.data(), s.size() };
}

std::vector<TString> ToString(const std::vector<std::string>& strings);
std::vector<std::string> ToStdString(const std::vector<TString>& strings);

////////////////////////////////////////////////////////////////////////////////

inline Poco::Timestamp ToTimestamp(TInstant t)
{
    return t.MicroSeconds();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
