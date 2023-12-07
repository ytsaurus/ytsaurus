#include "query_tracker.h"

#include "config.h"
#include "engine.h"
#include "ql_engine.h"
#include "yql_engine.h"
#include "chyt_engine.h"
#include "mock_engine.h"
#include "spyt_engine.h"

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_proxy.h>

#include <yt/yt/core/ypath/public.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/core/utilex/random.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NQueryTracker {

using namespace NAlertManager;
using namespace NApi;
using namespace NYPath;
using namespace NConcurrency;
using namespace NRecords;
using namespace NTableClient;
using namespace NLogging;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;

///////////////////////////////////////////////////////////////////////////////

static TLogger Logger("QueryTracker");

///////////////////////////////////////////////////////////////////////////////

class TQueryTracker
    : public IQueryTracker
{
public:
    TQueryTracker(
        TQueryTrackerDynamicConfigPtr config,
        TString selfAddress,
        IInvokerPtr controlInvoker,
        IClientPtr stateClient,
        TYPath stateRoot,
        int minRequiredStateVersion)
        : SelfAddress_(std::move(selfAddress))
        , ControlInvoker_(std::move(controlInvoker))
        , StateClient_(std::move(stateClient))
        , StateRoot_(std::move(stateRoot))
        , MinRequiredStateVersion_(minRequiredStateVersion)
        , AcquisitionExecutor_(New<TPeriodicExecutor>(ControlInvoker_, BIND(&TQueryTracker::AcquireQueries, MakeWeak(this))))
        , HealthCheckExecutor_(New<TPeriodicExecutor>(ControlInvoker_, BIND(&TQueryTracker::OnHealthCheck, MakeWeak(this)), config->HealthCheckPeriod))
    {
        Engines_[EQueryEngine::Mock] = CreateMockEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Ql] = CreateQlEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Yql] = CreateYqlEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Chyt] = CreateChytEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Spyt] = CreateSpytEngine(StateClient_, StateRoot_);
        // This is a correct call, despite being virtual call in constructor.
        TQueryTracker::Reconfigure(config);
    }

    void Start() override
    {
        AcquisitionExecutor_->Start();
        HealthCheckExecutor_->Start();
    }

    void Reconfigure(const TQueryTrackerDynamicConfigPtr& config) override
    {
        Config_ = config;
        AcquisitionExecutor_->SetPeriod(config->ActiveQueryAcquisitionPeriod);
        Engines_[EQueryEngine::Mock]->Reconfigure(config->MockEngine);
        Engines_[EQueryEngine::Ql]->Reconfigure(config->QlEngine);
        Engines_[EQueryEngine::Yql]->Reconfigure(config->YqlEngine);
        Engines_[EQueryEngine::Chyt]->Reconfigure(config->ChytEngine);
        Engines_[EQueryEngine::Spyt]->Reconfigure(config->SpytEngine);
    }

    void PopulateAlerts(std::vector<TAlert>* alerts) const override
    {
        WaitFor(
            BIND(&TQueryTracker::DoPopulateAlerts, MakeStrong(this), alerts)
                .AsyncVia(ControlInvoker_)
                .Run())
                .ThrowOnError();
    }

private:
    const TString SelfAddress_;
    const IInvokerPtr ControlInvoker_;
    const IClientPtr StateClient_;
    const TYPath StateRoot_;
    const int MinRequiredStateVersion_;

    const TPeriodicExecutorPtr AcquisitionExecutor_;
    const TPeriodicExecutorPtr HealthCheckExecutor_;

    std::vector<TAlert> Alerts_;
    TQueryTrackerDynamicConfigPtr Config_;

    THashMap<EQueryEngine, IQueryEnginePtr> Engines_;

    struct TAcquiredQuery
    {
        IQueryHandlerPtr Handler;
        i64 Incarnation;
    };

    THashMap<TQueryId, TAcquiredQuery> AcquiredQueries_;

    void DoPopulateAlerts(std::vector<TAlert>* alerts) const
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        alerts->insert(alerts->end(), Alerts_.begin(), Alerts_.end());
    }

    void OnHealthCheck()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_INFO("Requesting query tracker state version");
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::MasterCache;
        auto asyncResult = StateClient_->GetNode(StateRoot_ + "/@version", options);
        auto rspOrError = WaitFor(asyncResult);
        if (!rspOrError.IsOK()) {
            auto alert = TError(NAlerts::EErrorCode::QueryTrackerInvalidState, "Failed getting state version") << rspOrError;
            Alerts_ = {CreateAlert<NAlerts::EErrorCode>(alert)};
        } else {
            int stateVersion = ConvertTo<int>(rspOrError.Value());
            if (stateVersion < MinRequiredStateVersion_) {
                auto alert = TError(NAlerts::EErrorCode::QueryTrackerInvalidState, "Min required state version is not met")
                    << TErrorAttribute("version", stateVersion)
                    << TErrorAttribute("min_required_version", MinRequiredStateVersion_);
                Alerts_ = {CreateAlert<NAlerts::EErrorCode>(alert)};
            } else {
                Alerts_.clear();
            }
        }
     }

    void AcquireQueries()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContext = TTraceContext::NewRoot("QuerySelect");
        auto guard = TCurrentTraceContextGuard(traceContext);

        YT_LOG_INFO("Selecting queries with expired leases (AcquiredQueryCount: %v)", AcquiredQueries_.size());

        std::vector<TActiveQuery> queryRecords;

        try {
            // TODO(max42): select as little fields as possible; lookup full row in TryAcquireQuery instead.
            // Select queries with expired leases.
            auto selectQuery = Format(
                "[query_id], [incarnation], [assigned_tracker], [ping_time], [engine], [user], [query], [settings], [files] from [%v] where [ping_time] < %v",
                StateRoot_ + "/active_queries",
                (TInstant::Now() - Config_->ActiveQueryLeaseTimeout).MicroSeconds(),
                SelfAddress_);
            auto selectResult = WaitFor(StateClient_->SelectRows(selectQuery))
                .ValueOrThrow();
            queryRecords = ToRecords<TActiveQuery>(selectResult.Rowset);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error while selecting queries with expired leases");
            return;
        }
        YT_LOG_INFO("Queries with expired leases selected (ExpiredQueryCount: %v)", queryRecords.size());

        // Ensure even distribution of queries across trackers by introducing a random delay
        // between 0 and acquisition period.
        for (const auto& record : queryRecords) {
            auto delay = RandomDuration(Config_->ActiveQueryAcquisitionPeriod);
            if (AcquiredQueries_.contains(record.Key.QueryId)) {
                YT_LOG_INFO("Expired query is already acquired by us, doing nothing (QueryId: %v)", record.Key.QueryId);
            } else {
                YT_LOG_INFO(
                    "Scheduling acquisition of query (QueryId: %v, Engine: %v, User: %v, "
                    "Incarnation: %v, PingTime: %v, AssignedTracker: %v, Delay: %v)",
                    record.Key.QueryId,
                    record.Engine,
                    record.User,
                    record.Incarnation,
                    record.PingTime,
                    record.AssignedTracker,
                    delay);
                TDelayedExecutor::Submit(
                    BIND_NO_PROPAGATE(&TQueryTracker::TryAcquireQuery, MakeWeak(this), record),
                    delay,
                    ControlInvoker_);
            }
        }
    }

    void TryAcquireQuery(TActiveQuery queryRecord)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContext = TTraceContext::NewRoot("QueryAcquisition");
        auto guard = TCurrentTraceContextGuard(traceContext);

        try {
            GuardedTryAcquireQuery(std::move(queryRecord));
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error acquiring query");
        }
    }

    void GuardedTryAcquireQuery(TActiveQuery queryRecord)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto queryId = queryRecord.Key.QueryId;
        auto Logger = NQueryTracker::Logger.WithTag("QueryId: %v", queryId);
        YT_LOG_DEBUG("Starting acquisition transaction");
        auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        YT_LOG_DEBUG("Acquisition transaction started (TransactionId: %v)", transaction->GetId());

        const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
        auto optionalRecord = WaitFor(
            LookupActiveQuery(
                queryId,
                transaction->GetStartTimestamp(),
                {*idMapping.Incarnation, *idMapping.PingTime, *idMapping.AssignedTracker, *idMapping.State}))
            .ValueOrThrow();

        if (!optionalRecord) {
            YT_LOG_INFO("Query is no longer present (Timestamp: %v)", transaction->GetStartTimestamp());
            return;
        } else if (
            optionalRecord->Incarnation != queryRecord.Incarnation || optionalRecord->PingTime != queryRecord.PingTime)
        {
            YT_LOG_INFO(
                "Query was pinged by its acquirer (Incarnation: %v, PingTime: %v, AssignedTracker: %v, Timestamp: %v)",
                optionalRecord->Incarnation,
                optionalRecord->PingTime,
                optionalRecord->AssignedTracker,
                transaction->GetStartTimestamp());
            return;
        }
        auto newIncarnation = queryRecord.Incarnation + 1;
        YT_LOG_INFO(
            "Query is still expired, acquiring it (Timestamp: %v, Incarnation: %v, State: %v)",
            transaction->GetStartTimestamp(),
            newIncarnation,
            optionalRecord->State);

        // If current query state is "pending", switch it to "running". Otherwise, keep the existing state of a query;
        // in particular, it may be "failing" or "completing" if the previous incarnation succeeded in reaching pre-terminating state.
        auto newState = optionalRecord->State == EQueryState::Pending ? EQueryState::Running : optionalRecord->State;

        auto rowBuffer = New<TRowBuffer>();
        TActiveQueryPartial newRecord{
            .Key = queryRecord.Key,
            .State = newState,
            .Incarnation = newIncarnation,
            .PingTime = TInstant::Now(),
            .AssignedTracker = SelfAddress_,
        };
        std::vector newRows = {
            newRecord.ToUnversionedRow(rowBuffer, idMapping),
        };
        transaction->WriteRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(newRows), rowBuffer));
        auto commitResultOrError = WaitFor(transaction->Commit());
        if (!commitResultOrError.IsOK()) {
            YT_LOG_DEBUG(commitResultOrError, "Failed to acquire query");
            return;
        }
        // Do not forget to override incarnation into our new value.
        queryRecord.Incarnation = newIncarnation;
        YT_LOG_INFO(
            "Query acquired (CommitTimestamp: %v, State: %v)",
            commitResultOrError.Value().PrimaryCommitTimestamp,
            newState);

        IQueryHandlerPtr handler;
        try {
            handler = Engines_[queryRecord.Engine]->StartOrAttachQuery(queryRecord);
            handler->Start();
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Unrecoverable error on query start, finishing query");
            FinishQueryLoop(queryId, newIncarnation, TError(ex), EQueryState::Failed);
            return;
        }
        InsertOrCrash(AcquiredQueries_, std::pair{queryId, TAcquiredQuery{
            .Handler = std::move(handler),
            .Incarnation = newIncarnation,
        }});
        PingLoop(queryId, newIncarnation);
    }

    void PingLoop(TQueryId queryId, i64 incarnation)
    {
        while (true) {
            if (!TryPingQuery(queryId, incarnation)) {
                break;
            }
            auto backoffDuration = RandomDuration(Config_->ActiveQueryPingPeriod) + Config_->ActiveQueryPingPeriod / 2.0;
            TDelayedExecutor::WaitForDuration(backoffDuration);
        }
    }

    //! Ping query assuming it is of given incarnation. Returns true if pinging must continue and false otherwise.
    bool TryPingQuery(TQueryId queryId, i64 incarnation)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto Logger = QueryTrackerLogger.WithTag("QueryId: %v, Incarnation: %v", queryId, incarnation);

        if (auto iter = AcquiredQueries_.find(queryId);
            iter == AcquiredQueries_.end() || iter->second.Incarnation != incarnation)
        {
            YT_LOG_DEBUG("Cancelling obsolete ping");
            return false;
        }

        try {
            YT_LOG_DEBUG("Starting ping transaction");
            auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            YT_LOG_DEBUG("Ping transaction started (TransactionId: %v)", transaction->GetId());
            const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
            auto activeQueryRecord = WaitFor(
                LookupActiveQuery(
                    queryId,
                    transaction->GetStartTimestamp(),
                    {*idMapping.Incarnation, *idMapping.PingTime, *idMapping.State, *idMapping.AbortRequest, *idMapping.Error}))
                .ValueOrThrow();

            if (!activeQueryRecord) {
                YT_LOG_INFO("Query record is missing, cancelling ping");
                return false;
            }

            try {
                ValidateLease(incarnation, *activeQueryRecord, Config_->ActiveQueryLeaseTimeout);
            } catch (const std::exception& ex) {
                YT_LOG_INFO(
                    ex,
                    "Query lease has expired, detaching query (NewIncarnation: %v, NewPingTime: %v)",
                    activeQueryRecord->Incarnation,
                    activeQueryRecord->PingTime);
                DetachQuery(queryId);
                return false;
            }

            if (IsPreFinishedState(activeQueryRecord->State)) {
                YT_LOG_INFO("Query is in pre-terminating state, pinging stopped");
                TError error;
                EQueryState finalState;
                switch (activeQueryRecord->State) {
                    case EQueryState::Aborting:
                        error = ConvertTo<TError>(*activeQueryRecord->AbortRequest);
                        finalState = EQueryState::Aborted;
                        // XXX: is this safe? What if query was already removed from the map?
                        AcquiredQueries_[queryId].Handler->Abort();
                        YT_LOG_INFO("Query abort was requested (Error: %v)", error);
                        break;
                    case EQueryState::Failing:
                        error = *activeQueryRecord->Error;
                        finalState = EQueryState::Failed;
                        YT_LOG_INFO("Query failed (Error: %v)", error);
                        break;
                    case EQueryState::Completing:
                        finalState = EQueryState::Completed;
                        YT_LOG_INFO("Query completed");
                        break;
                    default:
                        YT_ABORT();
                }
                FinishQueryLoop(queryId, incarnation, error, finalState);
                DetachQuery(queryId);
                return false;
            }

            auto rowBuffer = New<TRowBuffer>();
            TActiveQueryPartial newRecord{
                .Key = TActiveQueryKey{.QueryId = queryId},
                .Incarnation = incarnation,
                .PingTime = TInstant::Now(),
            };
            std::vector newRows = {
                newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetIdMapping()),
            };
            transaction->WriteRows(
                StateRoot_ + "/active_queries",
                TActiveQueryDescriptor::Get()->GetNameTable(),
                MakeSharedRange(std::move(newRows), std::move(rowBuffer)));
            auto commitResultOrError = WaitFor(transaction->Commit());

            if (commitResultOrError.FindMatching(NTabletClient::EErrorCode::TransactionLockConflict)) {
                YT_LOG_INFO(
                    commitResultOrError,
                    "Ping transaction resulted in lock conflict, detaching query");
                DetachQuery(queryId);
                return false;
            }
            commitResultOrError.ThrowOnError();
            YT_LOG_DEBUG("Query pinged (CommitTimestamp: %v)", commitResultOrError.Value().PrimaryCommitTimestamp);
            return true;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error pinging query");
            return true;
        }
    }

    void DetachQuery(TQueryId queryId)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);
        if (auto it = AcquiredQueries_.find(queryId); it != AcquiredQueries_.end()) {
            const auto& [queryId, query] = *it;
            YT_LOG_INFO("Query detached (QueryId: %v)", queryId);
            query.Handler->Detach();
            AcquiredQueries_.erase(it);
        }
    }

    TFuture<std::optional<TActiveQuery>> LookupActiveQuery(TQueryId queryId, TTimestamp timestamp, TColumnFilter columnFilter = {})
    {
        TLookupRowsOptions options;
        options.Timestamp = timestamp;
        options.ColumnFilter = columnFilter;
        options.EnablePartialResult = true;
        TActiveQueryKey key{.QueryId = queryId};
        auto rowBuffer = New<TRowBuffer>();
        std::vector keys{
            key.ToKey(rowBuffer),
        };
        auto asyncLookupResult = StateClient_->LookupRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(keys), std::move(rowBuffer)),
            options);
        return asyncLookupResult.Apply(BIND([] (const TUnversionedLookupRowsResult& result) {
            auto optionalRecords = ToOptionalRecords<TActiveQuery>(result.Rowset);
            YT_VERIFY(optionalRecords.size() == 1);
            return optionalRecords[0];
        }));
    }

    void FinishQueryLoop(TQueryId queryId, i64 incarnation, TError error, EQueryState finalState)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        if (finalState == EQueryState::Aborted || finalState == EQueryState::Failed) {
            error = TError("Query %v %lv", queryId, finalState)
                << error
                << TErrorAttribute("query_id", queryId);
        }

        while (true) {
            if (!TryFinishQuery(queryId, incarnation, error, finalState)) {
                break;
            }
            auto backoffDuration = RandomDuration(Config_->QueryFinishBackoff) + Config_->QueryFinishBackoff / 2.0;
            TDelayedExecutor::WaitForDuration(backoffDuration);
        }
    }

    //! Finishes query by atomically moving its record from active to finished query table.
    //! Returns true if finishing was not successful and must be retried, and false otherwise
    //! (including situations when we lost lease and finishing must not be retried).
    bool TryFinishQuery(TQueryId queryId, i64 incarnation, TError error, EQueryState finalState)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        auto Logger = NQueryTracker::Logger.WithTag("QueryId: %v", queryId);

        try {
            YT_LOG_DEBUG("Starting finish transaction");
            auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            YT_LOG_DEBUG("Finish transaction started (TransactionId: %v)", transaction->GetId());

            auto activeQueryRecord = WaitFor(
                LookupActiveQuery(
                    queryId,
                    transaction->GetStartTimestamp()))
                .ValueOrThrow();

            if (!activeQueryRecord) {
                YT_LOG_INFO("Query record is missing, cancelling finish");
                return false;
            }

            try {
                ValidateLease(incarnation, *activeQueryRecord, Config_->ActiveQueryLeaseTimeout);
            } catch (const std::exception& ex) {
                YT_LOG_INFO(ex, "Query lease has expired, giving up finishing (NewIncarnation: %v, NewPingTime: %v)", activeQueryRecord->Incarnation, activeQueryRecord->PingTime);
                return false;
            }

            auto rowBuffer = New<TRowBuffer>();

            {
                std::vector keysToDelete = {
                    TActiveQueryKey{.QueryId = queryId}.ToKey(rowBuffer),
                };
                transaction->DeleteRows(
                    StateRoot_ + "/active_queries",
                    TActiveQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(keysToDelete), rowBuffer));
            }

            {
                // We must copy all fields of active query except for incarnation, ping time, assigned query and abort request
                // (which do not matter for finished query) and filter factors field (which goes to finished_queries_by_start_time table).
                static_assert(TActiveQueryDescriptor::FieldCount == 19 && TFinishedQueryDescriptor::FieldCount == 14);
                TFinishedQuery newRecord{
                    .Key = TFinishedQueryKey{.QueryId = queryId},
                    .Engine = activeQueryRecord->Engine,
                    .Query = activeQueryRecord->Query,
                    .Files = activeQueryRecord->Files,
                    .Settings = activeQueryRecord->Settings,
                    .User = activeQueryRecord->User,
                    .AccessControlObject = activeQueryRecord->AccessControlObject,
                    .StartTime = activeQueryRecord->StartTime,
                    .State = finalState,
                    .Progress = activeQueryRecord->Progress,
                    .Error = error,
                    .ResultCount = activeQueryRecord->ResultCount,
                    .FinishTime = activeQueryRecord->FinishTime,
                    .Annotations = activeQueryRecord->Annotations,
                };
                std::vector newRows = {
                    newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/finished_queries",
                    TFinishedQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));
            }

            {
                static_assert(TActiveQueryDescriptor::FieldCount == 19 && TFinishedQueryByStartTimeDescriptor::FieldCount == 7);
                TFinishedQueryByStartTime newRecord{
                    .Key = TFinishedQueryByStartTimeKey{.StartTime = activeQueryRecord->StartTime, .QueryId = queryId},
                    .Engine = activeQueryRecord->Engine,
                    .User = activeQueryRecord->User,
                    .AccessControlObject = activeQueryRecord->AccessControlObject,
                    .State = finalState,
                    .FilterFactors = activeQueryRecord->FilterFactors,
                };
                std::vector newRows = {
                    newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryByStartTimeDescriptor::Get()->GetIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/finished_queries_by_start_time",
                    TFinishedQueryByStartTimeDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));
            }
            auto commitResultOrError = WaitFor(transaction->Commit());
            if (!commitResultOrError.IsOK()) {
                YT_LOG_ERROR(commitResultOrError, "Failed to finish query, backing off");
                return true;
            }
            YT_LOG_INFO("Query finished (CommitTimestamp: %v)", commitResultOrError.Value().PrimaryCommitTimestamp);
            return false;
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error while finishing query");
            return true;
        }
    }

    static void ValidateIncarnation(i64 expectedIncarnation, const TActiveQuery& record)
    {
        if (record.Incarnation != expectedIncarnation) {
            THROW_ERROR_EXCEPTION(
                NQueryTrackerClient::EErrorCode::IncarnationMismatch,
                "Query incarnation mismatch: expected %v, actual %v",
                expectedIncarnation,
                record.Incarnation)
                    << TErrorAttribute("expected_incarnation", expectedIncarnation)
                    << TErrorAttribute("actual_incarnation", record.Incarnation);
        }
    }

    static void ValidateLease(i64 expectedIncarnation, const TActiveQuery& record, TDuration leaseExpirationTimeout)
    {
        ValidateIncarnation(expectedIncarnation, record);
        if (record.PingTime + leaseExpirationTimeout < TInstant::Now()) {
                THROW_ERROR_EXCEPTION(
                    NQueryTrackerClient::EErrorCode::IncarnationMismatch,
                    "Query lease expired for incarnation %v",
                    expectedIncarnation)
                    << TErrorAttribute("incarnation", expectedIncarnation)
                    << TErrorAttribute("ping_time", record.PingTime);
        }
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryTracker)

IQueryTrackerPtr CreateQueryTracker(
    TQueryTrackerDynamicConfigPtr config,
    TString selfAddress,
    IInvokerPtr controlInvoker,
    IClientPtr stateClient,
    TYPath stateRoot,
    int minRequiredStateVersion)
{
    return New<TQueryTracker>(
        std::move(config),
        std::move(selfAddress),
        std::move(controlInvoker),
        std::move(stateClient),
        std::move(stateRoot),
        minRequiredStateVersion);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
