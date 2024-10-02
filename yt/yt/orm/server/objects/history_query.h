#pragma once

#include "db_schema.h"
#include "select_object_history_executor.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void EnsureTimeMode(TTimeInterval* interval, bool allowTimeModeConversion, EHistoryTimeMode dbTimeMode);
// Conversion between physical and logical time is inaccurate:
// timestamps have 1-second precision, and physical time has microsecond precision,
// so given time is floored to the closest corresponding time.
void EnsureTimeMode(THistoryTime* time, bool allowTimeModeConversion, EHistoryTimeMode dbTimeMode, bool ceil = false);

std::pair<TString, TSelectObjectHistoryContinuationTokenSchema> BuildHistoryQuery(
    NMaster::IBootstrap* bootstrap,
    const THistoryIndexTable* historyIndexTable,
    TObjectTypeValue typeValue,
    const TObjectKey& objectKey,
    const std::optional<TAttributeSelector>& distinctBySelector,
    const std::optional<TSelectObjectHistoryContinuationToken>& continuationToken,
    int limit,
    const TSelectObjectHistoryOptions& options,
    bool useIndex,
    bool restrictHistoryFilter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
