#pragma once

#include "public.h"

#include <yt/yt/client/table_client/versioned_row.h>

namespace NYT::NTableClient::NDetail {

////////////////////////////////////////////////////////////////////////////////

void TrimRow(
    TMutableVersionedRow row,
    TVersionedValue* endValues,
    TChunkedMemoryPool* memoryPool);

int GetReaderSchemaColumnCount(TRange<TColumnIdMapping> schemaIdMapping);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient::NDetail
