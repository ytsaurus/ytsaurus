#pragma once

#include "public.h"

#include <yt/server/clickhouse_server/interop/api.h>

#include <yt/ytlib/table_client/public.h>

namespace NYT {
namespace NClickHouse {

////////////////////////////////////////////////////////////////////////////////

void ConvertValue(NInterop::TValue& dst, const NTableClient::TUnversionedValue& src);

NInterop::TRow ConvertRow(const NTableClient::TUnversionedRow& src);

}   // namespace NClickHouse
}   // namespace NYT
