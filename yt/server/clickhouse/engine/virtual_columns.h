#pragma once

#include <yt/server/clickhouse/interop/api.h>

#include <Core/NamesAndTypes.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

const DB::NamesAndTypesList& ListSystemVirtualColumns();

NInterop::TSystemColumns GetSystemColumns(const DB::Names& names);

} // namespace NClickHouse
} // namespace NYT
