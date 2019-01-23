#pragma once

#include "table_schema.h"
#include "table.h"

#include <yt/ytlib/table_client/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

// YT native types

bool IsYtTypeSupported(NTableClient::EValueType valueType);

EClickHouseColumnType RepresentYtType(NTableClient::EValueType valueType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
