#pragma once

#include "public.h"

#include <yt/ytlib/object_client/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TObjectId GetObjectIdFromDataSplit(const TDataSplit& dataSplit);

TTableSchema GetTableSchemaFromDataSplit(const TDataSplit& dataSplit);

TOwningKey GetLowerBoundFromDataSplit(const TDataSplit& dataSplit);

TOwningKey GetUpperBoundFromDataSplit(const TDataSplit& dataSplit);

TKeyRange GetBothBoundsFromDataSplit(const TDataSplit& dataSplit);

TTimestamp GetTimestampFromDataSplit(const TDataSplit& dataSplit);

bool IsSorted(const TDataSplit& dataSplit);

void SetObjectId(TDataSplit* dataSplit, NObjectClient::TObjectId objectId);

void SetTableSchema(TDataSplit* dataSplit, const TTableSchema& tableSchema);

void SetLowerBound(TDataSplit* dataSplit, const TOwningKey & lowerBound);

void SetUpperBound(TDataSplit* dataSplit, const TOwningKey & upperBound);

void SetBothBounds(TDataSplit* dataSplit, const TKeyRange& keyRange);

void SetTimestamp(TDataSplit* dataSplit, TTimestamp timestamp);

// XXX(sandello): For testing purposes only.
void SetSorted(TDataSplit* dataSplit, bool isSorted);

NLogging::TLogger MakeQueryLogger(TConstBaseQueryPtr query);

size_t GetSignificantWidth(TRow row);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

