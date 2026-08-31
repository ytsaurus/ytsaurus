#include "query_tracker.h"

#include "config.h"
#include "engine.h"
#include "profiler.h"
#include "ql_engine.h"
#include "yql_engine.h"
#include "chyt_engine.h"
#include "mock_engine.h"
#include "spyt_engine.h"
#include "search_index.h"
#include "helpers.h"

#include <yt/yt/server/lib/component_state_checker/state_checker.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/transaction_client/transaction_manager.h>

#include <yt/yt/ytlib/query_tracker_client/records/query.record.h>
#include <yt/yt/ytlib/query_tracker_client/helpers.h>

#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/object_client/helpers.h>

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
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NQueryTrackerClient::NRecords;
using namespace NComponentStateChecker;
using namespace NTableClient;
using namespace NLogging;
using namespace NTransactionClient;
using namespace NYTree;
using namespace NYson;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger("QueryTracker");

////////////////////////////////////////////////////////////////////////////////

class TQueryTracker
    : public IQueryTracker
{
public:
    TQueryTracker(
        TQueryTrackerDynamicConfigPtr config,
        std::string selfAddress,
        IInvokerPtr controlInvoker,
        IAlertCollectorPtr alertCollector,
        NApi::NNative::IClientPtr stateClient,
        IComponentStateCheckerPtr ComponentStateChecker,
        TYPath stateRoot,
        int minRequiredStateVersion)
        : SelfAddress_(std::move(selfAddress))
        , ControlInvoker_(std::move(controlInvoker))
        , AlertCollector_(std::move(alertCollector))
        , StateClient_(std::move(stateClient))
        , ComponentStateChecker_(std::move(ComponentStateChecker))
        , StateRoot_(std::move(stateRoot))
        , MinRequiredStateVersion_(minRequiredStateVersion)
        , AcquisitionExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TQueryTracker::AcquireQueries, MakeWeak(this))))
        , HealthCheckExecutor_(New<TPeriodicExecutor>(
            ControlInvoker_,
            BIND(&TQueryTracker::OnHealthCheck, MakeWeak(this)),
            config->HealthCheckPeriod))
        , TimeBasedIndex_(CreateTimeBasedIndex(StateClient_, StateRoot_))
    {
        Engines_[EQueryEngine::Mock] = CreateMockEngine(StateClient_, StateRoot_);
        Engines_[EQueryEngine::Ql] = CreateQLEngine(StateClient_, StateRoot_);
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

        ControlInvoker_->Invoke(BIND(&TQueryTracker::StartLeaseTransaction, MakeWeak(this)));
    }

    void Reconfigure(const TQueryTrackerDynamicConfigPtr& config) override
    {
        Config_ = config;
        AcquisitionExecutor_->SetPeriod(config->ActiveQueryAcquisitionPeriod);

        auto engines = {
            EQueryEngine::Mock,
            EQueryEngine::Ql,
            EQueryEngine::Yql,
            EQueryEngine::Chyt,
            EQueryEngine::Spyt,
        };
        for (const auto engine : engines) {
            Engines_[engine]->Reconfigure(GetConfigByEngine(Config_, engine));
        }
    }

    IYPathServicePtr GetOrchidService() const override
    {
        auto producer = BIND(&TQueryTracker::DoBuildOrchid, MakeStrong(this));
        return IYPathService::FromProducer(producer);
    }

    std::unordered_map<EQueryEngine, IProxyEngineProviderPtr> GetEngineProviders() override
    {
        std::unordered_map<EQueryEngine, IProxyEngineProviderPtr> engineProviders;
        for (const auto& engine : Engines_) {
            auto maybeEngineProvider = engine.second->GetProxyEngineProvider();
            if (maybeEngineProvider) {
                engineProviders[engine.first] = *maybeEngineProvider;
            }
        }
        return engineProviders;
    }

private:
    const std::string SelfAddress_;
    const IInvokerPtr ControlInvoker_;
    const IAlertCollectorPtr AlertCollector_;
    const NApi::NNative::IClientPtr StateClient_;
    const IComponentStateCheckerPtr ComponentStateChecker_;
    const TYPath StateRoot_;
    const int MinRequiredStateVersion_;

    const TPeriodicExecutorPtr AcquisitionExecutor_;
    const TPeriodicExecutorPtr HealthCheckExecutor_;

    const ISearchIndexPtr TimeBasedIndex_;

    NApi::ITransactionPtr LeaseTransaction_;

    TQueryTrackerDynamicConfigPtr Config_;

    THashMap<EQueryEngine, IQueryEnginePtr> Engines_;

    struct TAcquiredQuery
    {
        IQueryHandlerPtr Handler;
        i64 Incarnation;
        TTransactionId LeaseTransactionId;
    };

    THashMap<TQueryId, TAcquiredQuery> AcquiredQueries_;

    std::atomic<int> AcquisitionIterations_ = 0;

    void OnHealthCheck()
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        YT_TLOG_INFO("Requesting query tracker state version");
        TGetNodeOptions options;
        options.ReadFrom = EMasterChannelKind::Cache;
        auto asyncResult = StateClient_->GetNode(StateRoot_ + "/@version", options);
        auto rspOrError = WaitFor(asyncResult);
        if (!rspOrError.IsOK()) {
            AlertCollector_->StageAlert(CreateAlert(
                NAlerts::EErrorCode::QueryTrackerInvalidState,
                "Erroneous query tracker state",
                /*tags*/ {},
                rspOrError));
        } else {
            int stateVersion = ConvertTo<int>(rspOrError.Value());
            if (stateVersion < MinRequiredStateVersion_) {
                auto alert = TError(NAlerts::EErrorCode::QueryTrackerInvalidState, "Min required state version is not met")
                    .With("version", stateVersion)
                    .With("min_required_version", MinRequiredStateVersion_);
                AlertCollector_->StageAlert(CreateAlert(
                    NAlerts::EErrorCode::QueryTrackerInvalidState,
                    "Erroneous query tracker state",
                    /*tags*/ {},
                    alert));
            }
        }

        AlertCollector_->PublishAlerts();
    }

    void AcquireQueries()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        AcquisitionIterations_.fetch_add(1);

        auto traceContext = TTraceContext::NewRoot("QuerySelect");
        auto guard = TCurrentTraceContextGuard(traceContext);

        if (!LeaseTransaction_) {
            YT_TLOG_DEBUG("Skip active queries acquisition, since lease transaction is not started");
            return;
        }

        if (ComponentStateChecker_->IsComponentBanned()) {
            YT_TLOG_DEBUG("Skip active queries acquisition, since query tracker instance is banned");
            return;
        }

        YT_TLOG_DEBUG("Selecting active queries for potential acquisition");

        std::vector<TActiveQuery> queryRecords;

        try {
            // TODO(max42): select as little fields as possible; lookup full row in TryAcquireQuery instead.
            // Select queries with expired leases.
            auto selectQuery = Format(
                "[query_id], [incarnation], [assigned_tracker], [lease_transaction_id], [engine], [state], [user], [query], [settings], [files], [secrets], [access_control_objects], [is_indexed] from [%v]",
                StateRoot_ + "/active_queries");
            auto selectResult = WaitFor(StateClient_->SelectRows(selectQuery))
                .ValueOrThrow();
            queryRecords = ToRecords<TActiveQuery>(selectResult.Rowset);
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error while selecting queries with expired leases")
                .With(ex);
            return;
        }

        YT_TLOG_DEBUG("Active queries selected")
            .With("ActiveQueryCount", queryRecords.size());

        THashSet<TTransactionId> leaseTransactionIds;
        for (const auto& record : queryRecords) {
            leaseTransactionIds.insert(record.LeaseTransactionId);
        }

        THashSet<TTransactionId> activeLeaseTransactionIds;
        try {
            activeLeaseTransactionIds = WaitFor(GetAliveTransactions(leaseTransactionIds))
                .ValueOrThrow();
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error while getting alive lease transactions for active queries")
                .With(ex);
            return;
        }

        // Save profile counters.
        THashMap<TProfilingTags, int> activeQueryCounts;

        LeakySingleton<TActiveQueriesProfilingCountersMap>()->Flush();
        LeakySingleton<TActiveQueriesProfilingCountersMap>()->IterateReadOnly([&](const TProfilingTags& tags, const TActiveQueriesProfilingCounter&) {
            activeQueryCounts[tags] = 0;
        });
        for (const auto& record : queryRecords) {
            ++activeQueryCounts[ProfilingTagsFromActiveQueryRecord(record)];
        }
        for (const auto&[tags, count] : activeQueryCounts) {
            auto& activeQueriesCounter = GetOrCreateProfilingCounter<TActiveQueriesProfilingCounter>(
                QueryTrackerProfilerGlobal,
                tags)->ActiveQueries;
            activeQueriesCounter.Update(count);
        }

        std::vector<TActiveQuery> orphanedQueries;
        for (const auto& record : queryRecords) {
            if (!activeLeaseTransactionIds.contains(record.LeaseTransactionId)) {
                orphanedQueries.push_back(record);
            }
        }

        YT_TLOG_INFO("Selected orphaned active queries")
            .With("OrphanedQueryCount", orphanedQueries.size());

        // Ensure even distribution of queries across trackers by introducing a random delay
        // between 0 and acquisition period.
        for (const auto& record : orphanedQueries) {
            auto delay = RandomDuration(Config_->ActiveQueryAcquisitionPeriod);
            YT_TLOG_INFO("Scheduling acquisition of query")
                .With("QueryId", record.Key.QueryId)
                .With("Engine", record.Engine)
                .With("User", record.User)
                .With("Incarnation", record.Incarnation)
                .With("LeaseTransactionId", record.LeaseTransactionId)
                .With("AssignedTracker", record.AssignedTracker)
                .With("Delay", delay);
            TDelayedExecutor::Submit(
                BIND_NO_PROPAGATE(&TQueryTracker::TryAcquireQuery, MakeWeak(this), record),
                delay,
                ControlInvoker_);
        }
    }

    void TryAcquireQuery(TActiveQuery queryRecord)
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        auto traceContext = TTraceContext::NewRoot("QueryAcquisition");
        auto guard = TCurrentTraceContextGuard(traceContext);

        try {
            GuardedTryAcquireQuery(std::move(queryRecord));
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error acquiring query")
                .With(ex);
        }
    }

    void GuardedTryAcquireQuery(TActiveQuery queryRecord)
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        auto queryId = queryRecord.Key.QueryId;
        auto Logger = NQueryTracker::Logger().WithTag("QueryId", queryId);
        YT_TLOG_DEBUG("Starting acquisition transaction");
        auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
            .ValueOrThrow();
        YT_TLOG_DEBUG("Acquisition transaction started")
            .With("TransactionId", transaction->GetId());

        const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
        auto optionalRecord = WaitFor(
            LookupActiveQuery(
                queryId,
                transaction->GetStartTimestamp(),
                {idMapping.Incarnation, idMapping.LeaseTransactionId, idMapping.AssignedTracker, idMapping.State}))
            .ValueOrThrow();

        if (!LeaseTransaction_) {
            YT_TLOG_INFO("Failed to acquire query since lease transaction is not active");
            return;
        }

        auto leaseTransactionId = LeaseTransaction_->GetId();

        if (!optionalRecord) {
            YT_TLOG_INFO("Query is no longer present")
                .With("Timestamp", transaction->GetStartTimestamp());
            return;
        } else if (optionalRecord->Incarnation != queryRecord.Incarnation) {
            YT_TLOG_INFO("Query was already acquired by another entity")
                .With("Incarnation", optionalRecord->Incarnation)
                .With("LeaseTransactionId", optionalRecord->LeaseTransactionId)
                .With("AssignedTracker", optionalRecord->AssignedTracker)
                .With("Timestamp", transaction->GetStartTimestamp());
            return;
        }

        // If incarnation was not changed, query must have that same (dead) lease transaction.
        YT_VERIFY(optionalRecord->LeaseTransactionId == queryRecord.LeaseTransactionId);

        auto newIncarnation = queryRecord.Incarnation + 1;

        YT_TLOG_INFO("Query is still expired, acquiring it")
            .With("Timestamp", transaction->GetStartTimestamp())
            .With("Incarnation", newIncarnation)
            .With("LeaseTransactionId", leaseTransactionId)
            .With("State", optionalRecord->State);

        // If current query state is "running", switch it to "pending". Otherwise, keep the existing state of a query;
        // in particular, it may be "failing" or "completing" if the previous incarnation succeeded in reaching pre-terminating state.
        auto newState = optionalRecord->State == EQueryState::Running ? EQueryState::Pending : optionalRecord->State;
        std::optional<TError> acquisitionError;
        auto engine = Engines_[queryRecord.Engine];

        bool hasPreviousNonFinishingRun =
            queryRecord.LeaseTransactionId != NullTransactionId &&
            !IsFinishingState(newState);

        if (hasPreviousNonFinishingRun && !engine->IsSafeToRestartQuery()) {
            newState = EQueryState::Failing;

            auto error = TError("Query lease was lost; restarting query execution is unsafe")
                .With("previous_incarnation", queryRecord.Incarnation)
                .With("previous_lease_transaction_id", queryRecord.LeaseTransactionId);

            if (queryRecord.AssignedTracker) {
                error = std::move(error)
                    .With("previous_assigned_tracker", *queryRecord.AssignedTracker);
            }

            acquisitionError = std::move(error);
        }

        if (hasPreviousNonFinishingRun) {
            // Ensure that we don't track this query, so query acquisition
            // using same query tracker instance which dropped lease is safe.
            DetachQuery(queryId);
        }

        auto rowBuffer = New<TRowBuffer>();
        TActiveQueryPartial newRecord{
            .Key = queryRecord.Key,
            .State = newState,
            .Incarnation = newIncarnation,
            .LeaseTransactionId = leaseTransactionId,
            .AssignedTracker = SelfAddress_,
        };

        if (acquisitionError) {
            newRecord.Error = acquisitionError;
            newRecord.FinishTime = TInstant::Now();
        }

        std::vector newRows{
            newRecord.ToUnversionedRow(rowBuffer, TActiveQueryDescriptor::Get()->GetPartialIdMapping()),
        };
        transaction->WriteRows(
            StateRoot_ + "/active_queries",
            TActiveQueryDescriptor::Get()->GetNameTable(),
            MakeSharedRange(std::move(newRows), rowBuffer));
        auto commitResultOrError = WaitFor(transaction->Commit());
        if (!commitResultOrError.IsOK()) {
            YT_TLOG_DEBUG("Failed to acquire query")
                .With(commitResultOrError);
            return;
        }

        // This is a rare but possible race: lease transaction was aborted during write into the table.
        // Just do nothing: other query tracker (or even us) will find a query with dead lease transaction
        // and will try to acquire it.
        if (!LeaseTransaction_ || LeaseTransaction_->GetId() != leaseTransactionId) {
            YT_TLOG_INFO("Failed to acquire query since lease transaction was aborted during acquisition")
                .With("LeaseTransactionId", leaseTransactionId);
            return;
        }

        // Do not forget to update query record with new values.
        queryRecord.Incarnation = newIncarnation;
        queryRecord.LeaseTransactionId = leaseTransactionId;
        YT_TLOG_INFO("Query acquired")
            .With("CommitTimestamp", commitResultOrError.Value().PrimaryCommitTimestamp)
            .With("Incarnation", newIncarnation)
            .With("LeaseTransactionId", leaseTransactionId);

        IQueryHandlerPtr handler;
        if (!IsFinishingState(newState)) {
            try {
                handler = engine->StartOrAttachQuery(queryRecord);
                handler->Start();
            } catch (const std::exception& ex) {
                YT_TLOG_INFO("Unrecoverable error on query start, finishing query")
                    .With(ex);
                FinishQueryLoop(queryId, TError(ex), EQueryState::Failed);
                return;
            }
            InsertOrCrash(AcquiredQueries_, std::pair{queryId, TAcquiredQuery{
                .Handler = std::move(handler),
                .Incarnation = newIncarnation,
                .LeaseTransactionId = leaseTransactionId,
            }});
        } else {
            InsertOrCrash(AcquiredQueries_, std::pair{queryId, TAcquiredQuery{
                .Handler = nullptr,
                .Incarnation = newIncarnation,
                .LeaseTransactionId = leaseTransactionId,
            }});

        }

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
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        auto Logger = QueryTrackerLogger()
            .WithTag("QueryId", queryId)
            .WithTag("Incarnation", incarnation);

        if (auto iter = AcquiredQueries_.find(queryId);
            iter == AcquiredQueries_.end() || iter->second.Incarnation != incarnation)
        {
            YT_TLOG_DEBUG("Cancelling obsolete ping");
            DetachQuery(queryId);
            return false;
        }

        try {
            YT_TLOG_DEBUG("Starting ping transaction");

            auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();

            YT_TLOG_DEBUG("Ping transaction started")
                .With("TransactionId", transaction->GetId());
            const auto& idMapping = TActiveQueryDescriptor::Get()->GetIdMapping();
            auto activeQueryRecord = WaitFor(
                LookupActiveQuery(
                    queryId,
                    transaction->GetStartTimestamp(),
                    {idMapping.Incarnation, idMapping.State, idMapping.AbortRequest, idMapping.Error}))
                .ValueOrThrow();

            if (!activeQueryRecord) {
                YT_TLOG_INFO("Query record is missing, cancelling ping");
                DetachQuery(queryId);
                return false;
            }

            if (IsFinishingState(activeQueryRecord->State)) {
                YT_TLOG_INFO("Query is in pre-terminating state, pinging stopped");
                TError error;
                EQueryState finalState;
                switch (activeQueryRecord->State) {
                    case EQueryState::Aborting:
                        error = ConvertTo<TError>(*activeQueryRecord->AbortRequest);
                        finalState = EQueryState::Aborted;
                        if (AcquiredQueries_[queryId].Handler) {
                            AcquiredQueries_[queryId].Handler->Abort();
                        }
                        YT_TLOG_INFO("Query abort was requested")
                            .With("Error", error);
                        break;
                    case EQueryState::Failing:
                        error = *activeQueryRecord->Error;
                        finalState = EQueryState::Failed;
                        YT_TLOG_INFO("Query failed")
                            .With("Error", error);
                        break;
                    case EQueryState::Completing:
                        finalState = EQueryState::Completed;
                        YT_TLOG_INFO("Query completed");
                        break;
                    default:
                        YT_ABORT();
                }
                FinishQueryLoop(queryId, error, finalState);
                DetachQuery(queryId);
                return false;
            }

            YT_TLOG_DEBUG("Query is still running, doing nothing");

            return true;
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error pinging query")
                .With(ex);
            return true;
        }
    }

    void DetachQuery(TQueryId queryId)
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);
        if (auto it = AcquiredQueries_.find(queryId); it != AcquiredQueries_.end()) {
            const auto& [queryId, query] = *it;
            YT_TLOG_INFO("Query detached")
                .With("QueryId", queryId);
            if (query.Handler) {
                query.Handler->Detach();
            }
            AcquiredQueries_.erase(it);
        }
    }

    TFuture<std::optional<TActiveQuery>> LookupActiveQuery(TQueryId queryId, TTimestamp timestamp, TColumnFilter columnFilter = {})
    {
        TLookupRowsOptions options;
        options.Timestamp = timestamp;
        options.ColumnFilter = columnFilter;
        options.KeepMissingRows = true;
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

    void FinishQueryLoop(TQueryId queryId, TError error, EQueryState finalState)
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        if (finalState == EQueryState::Aborted || finalState == EQueryState::Failed) {
            error = TError("Query %v %lv", queryId, finalState)
                .With(error)
                .With("query_id", queryId);
        }

        while (true) {
            if (!TryFinishQuery(queryId, error, finalState)) {
                break;
            }
            auto backoffDuration = RandomDuration(Config_->QueryFinishBackoff) + Config_->QueryFinishBackoff / 2.0;
            TDelayedExecutor::WaitForDuration(backoffDuration);
        }
    }

    //! Finishes query by atomically moving its record from active to finished query table.
    //! Returns true if finishing was not successful and must be retried, and false otherwise
    //! (including situations when we lost lease and finishing must not be retried).
    bool TryFinishQuery(TQueryId queryId, TError error, EQueryState finalState)
    {
        YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

        auto Logger = NQueryTracker::Logger().WithTag("QueryId", queryId);

        try {
            YT_TLOG_DEBUG("Starting finish transaction");
            auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            YT_TLOG_DEBUG("Finish transaction started")
                .With("TransactionId", transaction->GetId());

            auto activeQueryRecord = WaitFor(
                LookupActiveQuery(
                    queryId,
                    transaction->GetStartTimestamp()))
                .ValueOrThrow();

            if (!activeQueryRecord) {
                YT_TLOG_INFO("Query record is missing, cancelling finish");
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
                // See YQLOVERYT-307. We must remove tokens from settings for long-term storage
                auto settingsNode = ConvertToNode(activeQueryRecord->Settings)->AsMap();
                settingsNode->RemoveChild("tokens");

                // We must copy all fields of active query except for incarnation, ping time, assigned query and abort request
                // (which do not matter for finished query) and filter factors field (which goes to finished_queries_by_start_time,
                // finished_queries_by_user_and_start_time, finished_queries_by_aco_and_start_time tables).
                static_assert(TActiveQueryDescriptor::FieldCount == 23 && TFinishedQueryDescriptor::FieldCount == 19);
                TFinishedQueryPartial newRecord{
                    .Key = {.QueryId = queryId},
                    .Engine = activeQueryRecord->Engine,
                    .Query = activeQueryRecord->Query,
                    .Files = activeQueryRecord->Files,
                    .Settings = ConvertToYsonString(settingsNode),
                    .User = activeQueryRecord->User,
                    .AccessControlObjects = activeQueryRecord->AccessControlObjects.value_or(TYsonString(TString("[]"))),
                    .StartTime = activeQueryRecord->StartTime,
                    .State = finalState,
                    .Progress = activeQueryRecord->Progress,
                    .Error = error,
                    .ResultCount = activeQueryRecord->ResultCount,
                    .FinishTime = activeQueryRecord->FinishTime,
                    .Annotations = activeQueryRecord->Annotations,
                    .Secrets = activeQueryRecord->Secrets.value_or(TYsonString(TString("[]"))),
                    .AssignedTracker = activeQueryRecord->AssignedTracker,
                    .IsIndexed = activeQueryRecord->IsIndexed,
                    .IsTutorial = activeQueryRecord->IsTutorial,
                };
                if (!activeQueryRecord->IsIndexed) {
                    if (auto ttl = GetConfigByEngine(Config_, activeQueryRecord->Engine)->NotIndexedQueriesTtl) {
                        newRecord.Ttl = ttl->MilliSeconds();
                    }
                }
                std::vector newRows = {
                    newRecord.ToUnversionedRow(rowBuffer, TFinishedQueryDescriptor::Get()->GetPartialIdMapping()),
                };
                transaction->WriteRows(
                    StateRoot_ + "/finished_queries",
                    TFinishedQueryDescriptor::Get()->GetNameTable(),
                    MakeSharedRange(std::move(newRows), rowBuffer));

                if (activeQueryRecord->IsIndexed) {
                    TimeBasedIndex_->AddQuery(PartialRecordToQuery(newRecord), transaction);
                }
            }

            auto commitResultOrError = WaitFor(transaction->Commit());
            if (!commitResultOrError.IsOK()) {
                YT_TLOG_ERROR("Failed to finish query, backing off")
                    .With(commitResultOrError);
                return true;
            }

            {
                // Save profile counter.
                auto& stateTimeGauge = GetOrCreateProfilingCounter<TStateTimeProfilingCounter>(
                    QueryTrackerProfiler,
                    ProfilingTagsFromActiveQueryRecord(*activeQueryRecord))->StateTime;
                auto now = TInstant::Now();
                stateTimeGauge.Update(now - activeQueryRecord->FinishTime.value());
            }

            YT_TLOG_INFO("Query finished")
                .With("CommitTimestamp", commitResultOrError.Value().PrimaryCommitTimestamp);
            return false;
        } catch (const std::exception& ex) {
            YT_TLOG_ERROR("Error while finishing query")
                .With(ex);
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
                    .With("expected_incarnation", expectedIncarnation)
                    .With("actual_incarnation", record.Incarnation);
        }
    }

    // Lease transaction management.
    void StartLeaseTransaction()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        YT_VERIFY(!LeaseTransaction_);

        YT_TLOG_DEBUG("Starting lease transaction");

        auto transaction = WaitFor(StateClient_->StartTransaction(ETransactionType::Master))
            .ValueOrThrow();
        YT_VERIFY(!std::exchange(LeaseTransaction_, std::move(transaction)));

        YT_TLOG_DEBUG("Lease transaction started")
            .With("TransactionId", LeaseTransaction_->GetId());

        LeaseTransaction_->SubscribeAborted(BIND(
            &TQueryTracker::OnLeaseTransactionAborted,
            MakeWeak(this),
            LeaseTransaction_->GetId())
            .Via(ControlInvoker_));
    }

    void OnLeaseTransactionAborted(TTransactionId transactionId, const TError& error)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(ControlInvoker_);

        YT_TLOG_WARNING("Lease transaction aborted")
            .With("TransactionId", transactionId)
            .With(error);

        YT_VERIFY(LeaseTransaction_);
        LeaseTransaction_.Reset();

        StartLeaseTransaction();

        auto activeQueries = std::exchange(AcquiredQueries_, {});
        for (const auto& [queryId, query] : activeQueries) {
            if (query.LeaseTransactionId != transactionId) {
                YT_TLOG_WARNING("Active query has unexpected lease transaction id during lease transaction abort handling, detaching it")
                    .With("QueryId", queryId)
                    .With("LeaseTransactionId", query.LeaseTransactionId)
                    .With("ExpectedLeaseTransactionId", transactionId);
            }

            if (query.Handler) {
                query.Handler->Detach();
            }
        }
    }

    //! Returns the subset of transactions that are still alive.
    TFuture<THashSet<TTransactionId>> GetAliveTransactions(const THashSet<TTransactionId>& transactionIds)
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        auto proxy = CreateObjectServiceReadProxy(StateClient_, NApi::EMasterChannelKind::Follower);
        auto batchReq = proxy.ExecuteBatch();

        for (const auto& transactionId : transactionIds) {
            auto req = TCypressYPathProxy::Exists(FromObjectId(transactionId));
            req->Tag() = transactionId;
            batchReq->AddRequest(std::move(req));
        }

        return batchReq->Invoke().Apply(BIND([] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            THashSet<TTransactionId> aliveTransactions;

            for (const auto& [tag, rspOrError] : batchRsp->GetTaggedResponses<TCypressYPathProxy::TRspExists>()) {
                auto transactionId = std::any_cast<TTransactionId>(tag);

                const auto& rsp = rspOrError.ValueOrThrow();
                if (rsp->value()) {
                    InsertOrCrash(aliveTransactions, transactionId);
                }
            }

            return aliveTransactions;
        }));
    }

    void DoBuildOrchid(IYsonConsumer* consumer) const
    {
        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("acquisition_iterations").Value(AcquisitionIterations_.load())
            .EndMap();
    }
};

DEFINE_REFCOUNTED_TYPE(TQueryTracker)

////////////////////////////////////////////////////////////////////////////////

IQueryTrackerPtr CreateQueryTracker(
    TQueryTrackerDynamicConfigPtr config,
    std::string selfAddress,
    IInvokerPtr controlInvoker,
    IAlertCollectorPtr alertCollector,
    NApi::NNative::IClientPtr stateClient,
    IComponentStateCheckerPtr ComponentStateChecker,
    TYPath stateRoot,
    int minRequiredStateVersion)
{
    return New<TQueryTracker>(
        std::move(config),
        std::move(selfAddress),
        std::move(controlInvoker),
        std::move(alertCollector),
        std::move(stateClient),
        std::move(ComponentStateChecker),
        std::move(stateRoot),
        minRequiredStateVersion);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
