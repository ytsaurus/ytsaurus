#pragma once

#include "table_schema.h"
#include "table.h"

#include <yt/ytlib/table_client/public.h>

#include <DataTypes/IDataType.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// YT native types

bool IsYtTypeSupported(NTableClient::EValueType valueType);

EClickHouseColumnType RepresentYtType(NTableClient::EValueType valueType);

NTableClient::EValueType RepresentClickHouseType(const DB::DataTypePtr& type);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
