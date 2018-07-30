#pragma once

#include <yt/server/clickhouse/interop/table_schema.h>

#include <yt/ytlib/table_client/public.h>

#include <util/generic/strbuf.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

// YQL types

bool IsYqlTypeSupported(TStringBuf typeName);

NInterop::EColumnType RepresentYqlType(TStringBuf typeName);

NTableClient::EValueType GetYqlUnderlyingYtType(TStringBuf typeName);

////////////////////////////////////////////////////////////////////////////////

// YT native types

bool IsYtTypeSupported(NTableClient::EValueType valueType);

NInterop::EColumnType RepresentYtType(NTableClient::EValueType valueType);

} // namespace NClickHouse
} // namespace NYT
