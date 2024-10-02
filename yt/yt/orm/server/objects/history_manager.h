#pragma once

#include "public.h"
#include "db_schema.h"
#include "select_continuation.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IHistoryManager
    : TRefCounted
{
    virtual ~IHistoryManager() = default;

    virtual const THistoryIndexTable* GetPrimaryWriteTable() const = 0;

    // Returns nullptr in case additional write is not required.
    virtual const THistoryIndexTable* GetSecondaryWriteTable() const = 0;

    virtual bool IsBackgroundMigrationAllowed() const = 0;

    // Infers table from current state and continuation token.
    // Fails in case continuation token refers to deprecated source.
    virtual const THistoryIndexTable* GetReadTable(
        const TSelectObjectHistoryContinuationToken& continuationToken) const = 0;
    virtual bool ForceAllowTimeModeConversion() const = 0;

    virtual THistoryTime GetWatchlogHistoryTime() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHistoryManager)

////////////////////////////////////////////////////////////////////////////////

void ValidateHistoryTables(
    const THistoryEventsTable* historyEventsTable,
    const THistoryIndexTable* historyIndexTable,
    bool validateTimeIsLogical = false);

////////////////////////////////////////////////////////////////////////////////

IHistoryManagerPtr MakeDefaultHistoryManager(
    const TTransaction* transaction,
    const THistoryIndexTable* indexTable);

IHistoryManagerPtr MakeMigrationHistoryManager(
    const TTransaction* transaction,
    const THistoryIndexTable* currentTable,
    const THistoryIndexTable* targetTable);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
