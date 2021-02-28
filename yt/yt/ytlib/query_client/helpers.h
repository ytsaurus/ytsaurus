#pragma once

#include "public.h"

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/core/logging/log.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

NObjectClient::TObjectId GetObjectIdFromDataSplit(const TDataSplit& dataSplit);
NTabletClient::TTabletId GetTabletIdFromDataSplit(const TDataSplit& dataSplit);
NObjectClient::TCellId GetCellIdFromDataSplit(const TDataSplit& dataSplit);

TTableSchemaPtr GetTableSchemaFromDataSplit(const TDataSplit& dataSplit);

TLegacyOwningKey GetLowerBoundFromDataSplit(const TDataSplit& dataSplit);
TLegacyOwningKey GetUpperBoundFromDataSplit(const TDataSplit& dataSplit);
TKeyRange GetBothBoundsFromDataSplit(const TDataSplit& dataSplit);

TTimestamp GetTimestampFromDataSplit(const TDataSplit& dataSplit);

bool IsSorted(const TDataSplit& dataSplit);

void SetObjectId(TDataSplit* dataSplit, NObjectClient::TObjectId objectId);
void SetTabletId(TDataSplit* dataSplit, NTabletClient::TTabletId tabletId);
void SetCellId(TDataSplit* dataSplit, NObjectClient::TCellId cellId);

void SetTableSchema(TDataSplit* dataSplit, const TTableSchema& tableSchema);

void SetLowerBound(TDataSplit* dataSplit, const TLegacyOwningKey & lowerBound);
void SetUpperBound(TDataSplit* dataSplit, const TLegacyOwningKey & upperBound);
void SetBothBounds(TDataSplit* dataSplit, const TKeyRange& keyRange);

void SetTimestamp(TDataSplit* dataSplit, TTimestamp timestamp);

// XXX(sandello): For testing purposes only.
void SetSorted(TDataSplit* dataSplit, bool isSorted);

NLogging::TLogger MakeQueryLogger(TConstBaseQueryPtr query);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient

