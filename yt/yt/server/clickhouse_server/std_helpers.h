#pragma once

#include <yt/yt/core/misc/common.h>

#include <Core/Names.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::Names ToNames(const std::vector<TString>& columnNames);
std::vector<TString> ToVectorString(const DB::Names& columnNames);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
