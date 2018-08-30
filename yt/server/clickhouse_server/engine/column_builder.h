#pragma once

#include <yt/server/clickhouse_server/interop/api.h>

#include <Columns/IColumn.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

NInterop::IColumnBuilderPtr CreateColumnBuilder(
    NInterop::EColumnType type,
    DB::MutableColumnPtr column);

}   // namespace NClickHouse
}   // namespace NYT
