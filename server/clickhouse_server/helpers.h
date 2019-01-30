#pragma once

#include "private.h"

#include "public_ch.h"

#include <yt/client/table_client/public.h>

#include <Core/Field.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

DB::KeyCondition CreateKeyCondition(
    const DB::Context& context,
    const DB::SelectQueryInfo& queryInfo,
    const TClickHouseTableSchema& schema);

DB::Field ConvertToField(const NTableClient::TUnversionedValue& value);

void ConvertToFieldRow(const NTableClient::TUnversionedRow& row, DB::Field* field);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer

