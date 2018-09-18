#pragma once

#include <yt/server/clickhouse_server/interop/api.h>

#include <Core/NamesAndTypes.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

const DB::NamesAndTypesList& ListSystemVirtualColumns();

NInterop::TSystemColumns GetSystemColumns(const DB::Names& names);

} // namespace NClickHouse
} // namespace NYT
