#pragma once

#include "public.h"

#include "value.h"

#include <yt/ytlib/table_client/public.h>

namespace NYT {
namespace NClickHouseServer {
namespace NNative {

////////////////////////////////////////////////////////////////////////////////

void ConvertValue(TValue& dst, const NTableClient::TUnversionedValue& src);

TRow ConvertRow(const NTableClient::TUnversionedRow& src);

////////////////////////////////////////////////////////////////////////////////

} // namespace NNative
} // namespace NClickHouseServer
} // namespace NYT
