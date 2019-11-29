#pragma once

#include "table_schema.h"
#include "table.h"

#include <yt/ytlib/table_client/public.h>

#include <DataTypes/IDataType.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// YT native types

NTableClient::TLogicalTypePtr AdaptTypeToClickHouse(const NTableClient::TLogicalTypePtr& valueType);

bool IsYtTypeSupported(const NTableClient::TLogicalTypePtr& valueType);

EClickHouseColumnType RepresentYtType(const NTableClient::TLogicalTypePtr& valueType);

NTableClient::TLogicalTypePtr RepresentClickHouseType(const DB::DataTypePtr& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
