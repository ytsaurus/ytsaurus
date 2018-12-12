#pragma once

#include <yt/server/clickhouse_server/native/column_builder.h>

#include <Columns/IColumn.h>

namespace NYT::NClickHouseServer::NEngine {

////////////////////////////////////////////////////////////////////////////////

NNative::IColumnBuilderPtr CreateColumnBuilder(
    NNative::EColumnType type,
    DB::MutableColumnPtr column);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NEngine
