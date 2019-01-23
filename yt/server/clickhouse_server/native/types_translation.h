#pragma once

#include "table_schema.h"

#include <yt/ytlib/table_client/public.h>

#include <util/generic/strbuf.h>

namespace NYT::NClickHouseServer::NNative {

////////////////////////////////////////////////////////////////////////////////

// YT native types

bool IsYtTypeSupported(NTableClient::EValueType valueType);

EClickHouseColumnType RepresentYtType(NTableClient::EValueType valueType);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer::NNative
