#pragma once

#include "public.h"

#include <ytlib/object_client/public.h>

#include <ytlib/new_table_client/schema.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TObjectId GetObjectIdFromDataSplit(const TDataSplit& dataSplit);

TTableSchema GetTableSchemaFromDataSplit(const TDataSplit& dataSplit);

TKeyColumns GetKeyColumnsFromDataSplit(const TDataSplit& dataSplit);

void SetObjectId(TDataSplit* dataSplit, const NObjectClient::TObjectId& objectId);

void SetTableSchema(TDataSplit* dataSplit, const TTableSchema& tableSchema);

void SetKeyColumns(TDataSplit* dataSplit, const TKeyColumns& keyColumns);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

