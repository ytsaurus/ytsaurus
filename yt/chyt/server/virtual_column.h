#pragma once

#include "private.h"

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

extern const TString TableIndexColumnName;
extern const TString TableKeyColumnName;
extern const TString TablePathColumnName;

extern const DB::VirtualColumnsDescription VirtualColumns;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
