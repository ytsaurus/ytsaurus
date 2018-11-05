#pragma once

#include <yt/server/clickhouse_server/native/column_builder.h>

#include <Columns/IColumn.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

////////////////////////////////////////////////////////////////////////////////

NNative::IColumnBuilderPtr CreateColumnBuilder(
    NNative::EColumnType type,
    DB::MutableColumnPtr column);

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
