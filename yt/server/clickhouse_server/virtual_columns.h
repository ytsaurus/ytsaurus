#pragma once

#include "system_columns.h"

#include <Core/NamesAndTypes.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

const DB::NamesAndTypesList& ListSystemVirtualColumns();

TSystemColumns GetSystemColumns(const DB::Names& names);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
