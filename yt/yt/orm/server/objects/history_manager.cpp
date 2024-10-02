#include "history_manager.h"

#include "config.h"
#include "transaction.h"

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

void ValidateHistoryTables(
    const THistoryEventsTable* historyEventsTable,
    const THistoryIndexTable* historyIndexTable,
    bool validateTimeIsLogical)
{
    const auto& indexKey = historyIndexTable->GetKeyFields(/*filterEvaluatedFields*/ false);
    const auto& eventsKey = historyEventsTable->GetKeyFields(/*filterEvaluatedFields*/ false);

    THROW_ERROR_EXCEPTION_UNLESS(historyIndexTable->PrimaryTable == historyEventsTable,
        "History index table is misconfigured: it references wrong primary table");

    if (validateTimeIsLogical) {
        THROW_ERROR_EXCEPTION_UNLESS(
            historyEventsTable->TimeMode == EHistoryTimeMode::Logical &&
            historyIndexTable->TimeMode == EHistoryTimeMode::Logical,
            "Expected logical time for history");
    }

    auto formatKey = [] (const auto& key) {
        return MakeFormattableView(key, [] (TStringBuilderBase* builder, const TDBField* field) {
            FormatValue(builder, *field, TStringBuf());
        });
    };
    TError error = TError("History tables key mismatch: index table key is %v, events table key is %v",
        formatKey(indexKey),
        formatKey(eventsKey));

    bool historyHasHashField = historyEventsTable->HasHashKeyField;
    THROW_ERROR_EXCEPTION_UNLESS(historyIndexTable->HasHashKeyField == historyHasHashField, error);
    THROW_ERROR_EXCEPTION_UNLESS(historyIndexTable->UseUuidInKey == historyEventsTable->UseUuidInKey, error);
    THROW_ERROR_EXCEPTION_UNLESS(
        indexKey.size() == eventsKey.size() +
            /*indexEventType*/ 1 +
            /*historyEventType*/ historyEventsTable->UseUuidInKey,
        error);

    int startIndex = historyHasHashField;
    THROW_ERROR_EXCEPTION_UNLESS(*indexKey[startIndex] == *eventsKey[startIndex], error);
    for (int i = startIndex + 3; i < std::ssize(indexKey); ++i) {
        THROW_ERROR_EXCEPTION_UNLESS(*indexKey[i] == *eventsKey[i - 2], error);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

bool MatchesTable(const TString& readSourceName, const THistoryIndexTable* table)
{
    return readSourceName == table->GetName() || readSourceName == table->PrimaryTable->GetName();
}

[[noreturn]] void ThrowInvalidContinuationTokenSource(const TString& readSourceName)
{
    THROW_ERROR_EXCEPTION(NClient::EErrorCode::InvalidContinuationToken,
        "Continuation token requested %Qv read source which is invalid or deprecated",
        readSourceName);
}

////////////////////////////////////////////////////////////////////////////////

class TDefaultHistoryManager
    : public IHistoryManager
{
public:
    TDefaultHistoryManager(
        const TTransaction* owner,
        const THistoryIndexTable* table)
        : Owner_(owner)
        , Table_(table)
    { }

    const THistoryIndexTable* GetPrimaryWriteTable() const override
    {
        return Table_;
    }

    const THistoryIndexTable* GetSecondaryWriteTable() const override
    {
        return nullptr;
    }

    bool IsBackgroundMigrationAllowed() const override
    {
        YT_ABORT();
    }

    const THistoryIndexTable* GetReadTable(
        const TSelectObjectHistoryContinuationToken& continuationToken) const override
    {
        if (continuationToken.ReadPhase == EHistoryReadPhase::Uninitialized ||
            MatchesTable(continuationToken.ReadSource, Table_))
        {
            return Table_;
        }

        ThrowInvalidContinuationTokenSource(continuationToken.ReadSource);
    }

    bool ForceAllowTimeModeConversion() const override
    {
        return false;
    }

    THistoryTime GetWatchlogHistoryTime() const override
    {
        return Owner_->GetHistoryEventTime(Table_);
    }

private:
    const TTransaction* const Owner_;
    const THistoryIndexTable* const Table_;

};

////////////////////////////////////////////////////////////////////////////////

class TMigrationHistoryManager
    : public IHistoryManager
{
public:
    TMigrationHistoryManager(
        const TTransaction* owner,
        const THistoryIndexTable* currentTable,
        const THistoryIndexTable* targetTable)
        : Owner_(owner)
        , MigrationState_(Owner_->GetConfig()->HistoryMigrationState)
        , CurrentTable_(currentTable)
        , TargetTable_(targetTable)
    { }

    const THistoryIndexTable* GetPrimaryWriteTable() const override
    {
        switch (MigrationState_) {
            case EMigrationState::Initial:
            case EMigrationState::WriteBoth:
            case EMigrationState::AfterBackgroundMigration:
            case EMigrationState::ReadNew:
            case EMigrationState::RevokeOldTokens:
                return CurrentTable_;
            case EMigrationState::Target:
                return TargetTable_;
        }
    }

    const THistoryIndexTable* GetSecondaryWriteTable() const override
    {
        switch (MigrationState_) {
            case EMigrationState::Initial:
                return nullptr;
            case EMigrationState::WriteBoth:
            case EMigrationState::AfterBackgroundMigration:
            case EMigrationState::ReadNew:
            case EMigrationState::RevokeOldTokens:
                return TargetTable_;
            case EMigrationState::Target:
                return nullptr;
        }
    }

    bool IsBackgroundMigrationAllowed() const override
    {
        switch (MigrationState_) {
            case EMigrationState::Initial:
            case EMigrationState::WriteBoth:
                return true;
            case EMigrationState::AfterBackgroundMigration:
            case EMigrationState::ReadNew:
            case EMigrationState::RevokeOldTokens:
            case EMigrationState::Target:
                return false;
        }
    }

    const THistoryIndexTable* GetReadTable(
        const TSelectObjectHistoryContinuationToken& continuationToken) const override
    {
        if (continuationToken.ReadPhase == EHistoryReadPhase::Uninitialized) {
            switch (MigrationState_) {
                case EMigrationState::Initial:
                case EMigrationState::WriteBoth:
                case EMigrationState::AfterBackgroundMigration:
                    return CurrentTable_;
                case EMigrationState::ReadNew:
                case EMigrationState::RevokeOldTokens:
                case EMigrationState::Target:
                    return TargetTable_;
            }
        }

        switch (MigrationState_) {
            case EMigrationState::Initial:
            case EMigrationState::WriteBoth:
            case EMigrationState::AfterBackgroundMigration:
                if (MatchesTable(continuationToken.ReadSource, CurrentTable_)) {
                    return CurrentTable_;
                } else {
                    ThrowInvalidContinuationTokenSource(continuationToken.ReadSource);
                }

            case EMigrationState::ReadNew:
                if (MatchesTable(continuationToken.ReadSource, CurrentTable_)) {
                    return CurrentTable_;
                } else if (MatchesTable(continuationToken.ReadSource, TargetTable_)) {
                    return TargetTable_;
                } else {
                    ThrowInvalidContinuationTokenSource(continuationToken.ReadSource);
                }

            case EMigrationState::RevokeOldTokens:
            case EMigrationState::Target:
                if (MatchesTable(continuationToken.ReadSource, TargetTable_)) {
                    return TargetTable_;
                } else {
                    ThrowInvalidContinuationTokenSource(continuationToken.ReadSource);
                }
        }
    }

    bool ForceAllowTimeModeConversion() const override
    {
        switch (MigrationState_) {
            case EMigrationState::Initial:
                return false;
            case EMigrationState::WriteBoth:
            case EMigrationState::AfterBackgroundMigration:
            case EMigrationState::ReadNew:
            case EMigrationState::RevokeOldTokens:
                return true;
            case EMigrationState::Target:
                return false;
        }
    }

    THistoryTime GetWatchlogHistoryTime() const override
    {
        switch (MigrationState_) {
            case EMigrationState::Initial:
            case EMigrationState::WriteBoth:
            case EMigrationState::AfterBackgroundMigration:
                return Owner_->GetHistoryEventTime(CurrentTable_);
            case EMigrationState::ReadNew:
            case EMigrationState::RevokeOldTokens:
            case EMigrationState::Target:
                return Owner_->GetHistoryEventTime(TargetTable_);
        }
    }

private:
    const TTransaction* const Owner_;
    const EMigrationState MigrationState_;
    const THistoryIndexTable* const CurrentTable_;
    const THistoryIndexTable* const TargetTable_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

IHistoryManagerPtr MakeDefaultHistoryManager(
    const TTransaction* transaction,
    const THistoryIndexTable* indexTable)
{
    YT_VERIFY(transaction);
    return New<TDefaultHistoryManager>(transaction, indexTable);
}

IHistoryManagerPtr MakeMigrationHistoryManager(
    const TTransaction* transaction,
    const THistoryIndexTable* currentTable,
    const THistoryIndexTable* targetTable)
{
    YT_VERIFY(transaction);
    return New<TMigrationHistoryManager>(transaction, currentTable, targetTable);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
