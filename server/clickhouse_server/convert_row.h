#pragma once

#include "public.h"

#include "value.h"

#include <yt/ytlib/table_client/public.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

void ConvertValue(TValue& dst, const NTableClient::TUnversionedValue& src);

TRow ConvertRow(const NTableClient::TUnversionedRow& src);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
