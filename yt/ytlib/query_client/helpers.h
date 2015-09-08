#pragma once

#include "public.h"

#include <ytlib/object_client/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TObjectId GetObjectIdFromDataSplit(const TDataSplit& dataSplit);

TTableSchema GetTableSchemaFromDataSplit(const TDataSplit& dataSplit);

TKeyColumns GetKeyColumnsFromDataSplit(const TDataSplit& dataSplit);

TKey GetLowerBoundFromDataSplit(const TDataSplit& dataSplit);

TKey GetUpperBoundFromDataSplit(const TDataSplit& dataSplit);

TKeyRange GetBothBoundsFromDataSplit(const TDataSplit& dataSplit);

TTimestamp GetTimestampFromDataSplit(const TDataSplit& dataSplit);

bool IsSorted(const TDataSplit& dataSplit);

void SetObjectId(TDataSplit* dataSplit, const NObjectClient::TObjectId& objectId);

void SetTableSchema(TDataSplit* dataSplit, const TTableSchema& tableSchema);

void SetKeyColumns(TDataSplit* dataSplit, const TKeyColumns& keyColumns);

void SetLowerBound(TDataSplit* dataSplit, const TKey& lowerBound);

void SetUpperBound(TDataSplit* dataSplit, const TKey& upperBound);

void SetBothBounds(TDataSplit* dataSplit, const TKeyRange& keyRange);

void SetTimestamp(TDataSplit* dataSplit, TTimestamp timestamp);

// XXX(sandello): For testing purposes only.
void SetSorted(TDataSplit* dataSplit, bool isSorted);

////////////////////////////////////////////////////////////////////////////////

struct TRowRangeFormatter
{
    void operator ()(TStringBuilder* builder, const TRowRange& range) const;
};

struct TDataSourceFormatter
{
    void operator ()(TStringBuilder* builder, const TDataSource& source) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

