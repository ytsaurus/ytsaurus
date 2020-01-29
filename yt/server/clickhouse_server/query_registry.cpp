#include "query_registry.h"

#include "query_context.h"
#include "private.h"
#include "config.h"

#include <yt/core/ytree/ypath_service.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/crash_handler.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

static const auto& Logger = ServerLogger;

////////////////////////////////////////////////////////////////////////////////

struct TUserStatusSnapshot
{
    std::vector<const DB::QueryStatusInfo*> QueryStatuses;
    i64 TotalMemoryUsage = 0;
    // TODO(max42): expose peak memory usage from process list.

    void AddQueryStatus(const DB::QueryStatusInfo* queryStatus)
    {
        QueryStatuses.push_back(queryStatus);
        TotalMemoryUsage += queryStatus->memory_usage;
    }
};

void Serialize(const TUserStatusSnapshot& snapshot, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total_memory_usage").Value(snapshot.TotalMemoryUsage)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

//! Class that stores a snapshot of DB::ProcessList::getInfo() and provides
//! convenient accessors to its information.
class TProcessListSnapshot
{
public:
    TProcessListSnapshot(const DB::ProcessList& processList)
    {
        auto queryStatuses = processList.getInfo(true, true, true);
        // NB: reservation is important for stored pointers not to invalidate.
        QueryStatuses_.reserve(queryStatuses.size());
        for (auto& queryStatus : queryStatuses) {
            const auto& queryIdString = queryStatus.client_info.current_query_id;
            const auto& user = queryStatus.client_info.current_user;
            TQueryId queryId;
            if (!TQueryId::FromString(queryIdString, &queryId)) {
                YT_LOG_DEBUG("Process list contains query without proper YT query id, skipping it in query registry (QueryId: %v)", queryIdString);
            }
            QueryStatuses_.emplace_back(std::move(queryStatus));
            QueryIdToQueryStatus_[queryId] = &QueryStatuses_.back();
            UserToUserStatusSnapshot_[user].AddQueryStatus(&QueryStatuses_.back());
        }
        YT_LOG_INFO("Built process list snapshot (QueryStatusCount: %v, UserCount: %v)", QueryStatuses_.size(), UserToUserStatusSnapshot_.size());
    }

    TProcessListSnapshot() = default;

    const DB::QueryStatusInfo* FindQueryStatusByQueryId(const TQueryId& queryId) const
    {
        auto it = QueryIdToQueryStatus_.find(queryId);
        if (it != QueryIdToQueryStatus_.end()) {
            YT_LOG_ERROR("XXX Looking for query id %v, success", queryId);

            return it->second;
        }
        YT_LOG_ERROR("XXX Looking for query id %v, fail", queryId);
        return nullptr;
    }

    const TUserStatusSnapshot* FindUserStatusSnapshotByUser(const TString& user) const
    {
        auto it = UserToUserStatusSnapshot_.find(user);
        if (it != UserToUserStatusSnapshot_.end()) {
            return &it->second;
        }
        return nullptr;
    }

private:
    std::vector<DB::QueryStatusInfo> QueryStatuses_;
    THashMap<TQueryId, const DB::QueryStatusInfo*> QueryIdToQueryStatus_;
    THashMap<TString, TUserStatusSnapshot> UserToUserStatusSnapshot_;
};

////////////////////////////////////////////////////////////////////////////////

class TUserProfilingEntry
{
public:
    int RunningInitialQueryCount = 0;
    int RunningSecondaryQueryCount = 0;
    int HistoricalInitialQueryCount = 0;
    int HistoricalSecondaryQueryCount = 0;

    NProfiling::TTagId TagId;

    explicit TUserProfilingEntry(const TString& name)
        : TagId(TProfileManager::Get()->RegisterTag("user", name))
        , Name_(name)
    { }

private:
    TString Name_;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TUserProfilingEntry& userInfo, IYsonConsumer* consumer, const TUserStatusSnapshot* userStatusSnapshot)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("running_initial_query_count").Value(userInfo.RunningInitialQueryCount)
            .Item("running_secondary_query_count").Value(userInfo.RunningSecondaryQueryCount)
            .Item("historical_initial_query_count").Value(userInfo.HistoricalInitialQueryCount)
            .Item("historical_secondary_query_count").Value(userInfo.HistoricalSecondaryQueryCount)
            .Item("user_status").Value(userStatusSnapshot)
        .EndMap();
}

/////////////////////////////////////////////////////////////////////////////

//! It is pretty tricky to dump structure like TQueryRegistry in signal handler,
//! so we periodically dump its state to a circular buffer, which is stored in this class.
class TSignalSafeState
{
public:
    TSignalSafeState()
    {
        memset(&StateBuffer_, 0, sizeof(StateBuffer_));
    }

    void SaveState(TString stateString)
    {
        YT_LOG_DEBUG("Saving query registry state (StatePointer: %v)", StatePointer_);
        while (StateBuffer_[StatePointer_] != 0) {
            ++StatePointer_;
        }
        // Skip one more zero to keep previous string readable.
        ++StatePointer_;
        YT_LOG_DEBUG("Skipped previous string (StatePointer: %v)", StatePointer_);

        size_t remainingSize = StateAllocationSize_ - StatePointer_ - 1;
        YT_LOG_DEBUG("New query registry state built (StateSize: %v, RemainingSize: %v)", stateString.size(), remainingSize);
        if (remainingSize < stateString.size()) {
            YT_LOG_INFO("Not enough place for new query registry state, moving pointer to the beginning", StatePointer_);
            StatePointer_ = 0;
        }
        remainingSize = StateAllocationSize_ - StatePointer_ - 1;
        if (remainingSize < stateString.size()) {
            YT_LOG_ERROR("Query registry state is too large, it is going to be truncated (StateSize: %v, StateAllocationSize: %v)",
                stateString.size(),
                StateAllocationSize_);
            static const char* truncatedMarker = "...TRUNCATED";
            stateString.resize(StateAllocationSize_ - 13 /* sizeof(truncatedMarker)*/);
            stateString += truncatedMarker;
        }

        strcpy(&StateBuffer_[StatePointer_], stateString.data());
        YT_LOG_DEBUG("Query registry state saved (StatePointer: %v, Length: %v)", StatePointer_, stateString.size());
    }

    void WriteToStderr() const
    {
        using NYT::WriteToStderr;

        int startPosition = StatePointer_;
        int zeroPosition;
        for (zeroPosition = startPosition; StateBuffer_[zeroPosition]; ++zeroPosition);
        WriteToStderr("*** Query registry state ***\n");
        WriteToStderr(&StateBuffer_[startPosition], zeroPosition - startPosition);
        WriteToStderr("\n");
    }

private:
    static constexpr size_t StateAllocationSize_ = 128_MB;
    std::array<char, StateAllocationSize_> StateBuffer_;
    int StatePointer_ = 0;
};

class TQueryRegistry::TImpl
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, OrchidService);

    TImpl(TBootstrap* bootstrap)
        : OrchidService_(IYPathService::FromProducer(BIND(&TImpl::BuildYson, MakeWeak(this))))
        , Bootstrap_(bootstrap)
        , IdlePromise_(MakePromise<void>(TError()))
        , ProcessListSnapshotExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::UpdateProcessListSnapshot, MakeWeak(this)),
            Bootstrap_->GetConfig()->ProcessListSnapshotUpdatePeriod))
    { }

    void Start()
    {
        ProcessListSnapshotExecutor_->Start();
    }

    void Stop()
    {
        WaitFor(ProcessListSnapshotExecutor_->Stop())
            .ThrowOnError();
    }

    void Register(TQueryContext* queryContext)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        const auto& Logger = queryContext->Logger;

        YT_VERIFY(QueryContexts_.insert(queryContext).second);

        auto& userProfilingEntry = GetOrRegisterUserProfilingEntry(queryContext->User);

        switch (queryContext->QueryKind)
        {
            case EQueryKind::InitialQuery:
                ++userProfilingEntry.HistoricalInitialQueryCount;
                ++userProfilingEntry.RunningInitialQueryCount;
                break;
            case EQueryKind::SecondaryQuery:
                ++userProfilingEntry.HistoricalSecondaryQueryCount;
                ++userProfilingEntry.RunningSecondaryQueryCount;
                break;
            default:
                YT_ABORT();
        }

        UpdateProcessListSnapshot();
        SaveState();

        YT_LOG_INFO("Query registered");

        if (QueryContexts_.size() == 1) {
            IdlePromise_ = NewPromise<void>();
        }
    }

    void Unregister(TQueryContext* queryContext)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        const auto& Logger = queryContext->Logger;

        YT_VERIFY(QueryContexts_.erase(queryContext));

        auto& userProfilingEntry = GetOrCrash(UserToUserProfilingEntry_, queryContext->User);
        switch (queryContext->QueryKind)
        {
            case EQueryKind::InitialQuery:
                --userProfilingEntry.RunningInitialQueryCount;
                break;
            case EQueryKind::SecondaryQuery:
                --userProfilingEntry.RunningSecondaryQueryCount;
                break;
            default:
                YT_ABORT();
        }

        UpdateProcessListSnapshot();
        SaveState();

        YT_LOG_INFO("Query unregistered");

        if (QueryContexts_.empty()) {
            IdlePromise_.Set();
        }
    }

    size_t GetQueryCount() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        return QueryContexts_.size();
    }

    TFuture<void> GetIdleFuture() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        return IdlePromise_.ToFuture();
    }

    void OnProfiling() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        for (const auto& [user, userProfilingInfo] : UserToUserProfilingEntry_) {
            ServerProfiler.Enqueue(
                "/running_initial_query_count",
                userProfilingInfo.RunningInitialQueryCount,
                EMetricType::Gauge,
                {userProfilingInfo.TagId});

            ServerProfiler.Enqueue(
                "/running_secondary_query_count",
                userProfilingInfo.RunningSecondaryQueryCount,
                EMetricType::Gauge,
                {userProfilingInfo.TagId});

            ServerProfiler.Enqueue(
                "/historical_initial_query_count",
                userProfilingInfo.HistoricalInitialQueryCount,
                EMetricType::Counter,
                {userProfilingInfo.TagId});

            ServerProfiler.Enqueue(
                "/historical_secondary_query_count",
                userProfilingInfo.HistoricalSecondaryQueryCount,
                EMetricType::Counter,
                {userProfilingInfo.TagId});

            i64 memoryUsage = 0;
            if (const auto* userStatusSnapshot = ProcessListSnapshot_.FindUserStatusSnapshotByUser(user)) {
                memoryUsage = userStatusSnapshot->TotalMemoryUsage;
            }

            ServerProfiler.Enqueue(
                "/memory_usage",
                memoryUsage,
                EMetricType::Gauge,
                {userProfilingInfo.TagId});
        }
    }

    void WriteStateToStderr() const
    {
        SignalSafeState_.WriteToStderr();
    }

    void SaveState()
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        TStringStream stream;
        TYsonWriter writer(&stream, EYsonFormat::Pretty);
        BuildYson(&writer);
        auto result = stream.Str();

        SignalSafeState_.SaveState(result);
    }

    void UpdateProcessListSnapshot()
    {
        ProcessListSnapshot_ = TProcessListSnapshot(Bootstrap_->GetHost()->GetContext().getProcessList());
    }

private:
    TBootstrap* Bootstrap_;
    THashSet<TQueryContext*> QueryContexts_;

    THashMap<TString, TUserProfilingEntry> UserToUserProfilingEntry_;

    TPromise<void> IdlePromise_;

    TSignalSafeState SignalSafeState_;

    TProcessListSnapshot ProcessListSnapshot_;

    TPeriodicExecutorPtr ProcessListSnapshotExecutor_;

    void BuildYson(IYsonConsumer* consumer) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        BuildYsonFluently(consumer)
            .BeginMap()
            .Item("running_queries").DoMapFor(QueryContexts_, [&] (TFluentMap fluent, const TQueryContext* queryContext) {
                const auto& queryId = queryContext->QueryId;
                fluent
                    .Item(ToString(queryId)).Value(*queryContext, ProcessListSnapshot_.FindQueryStatusByQueryId(queryId));
            })
            .Item("users").DoMapFor(UserToUserProfilingEntry_, [&] (TFluentMap fluent, const auto& pair) {
                const auto& [user, userProfilingEntry] = pair;
                fluent
                    .Item(user).Value(userProfilingEntry, ProcessListSnapshot_.FindUserStatusSnapshotByUser(user));
            })
            .EndMap();
    }

    TUserProfilingEntry& GetOrRegisterUserProfilingEntry(const TString& user)
    {
        THashMap<TString, TUserProfilingEntry>::insert_ctx ctx;
        auto it = UserToUserProfilingEntry_.find(user, ctx);
        if (it == UserToUserProfilingEntry_.end()) {
            it = UserToUserProfilingEntry_.emplace_direct(ctx, user, TUserProfilingEntry(user));
        }
        return it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

TQueryRegistry::TQueryRegistry(NYT::NClickHouseServer::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

void TQueryRegistry::Register(TQueryContext* queryContext)
{
    Impl_->Register(queryContext);
}

void TQueryRegistry::Unregister(TQueryContext* queryContext)
{
    Impl_->Unregister(queryContext);
}

size_t TQueryRegistry::GetQueryCount() const
{
    return Impl_->GetQueryCount();
}

TFuture<void> TQueryRegistry::GetIdleFuture() const
{
    return Impl_->GetIdleFuture();
}

void TQueryRegistry::OnProfiling() const
{
    Impl_->OnProfiling();
}

IYPathServicePtr TQueryRegistry::GetOrchidService() const
{
    return Impl_->GetOrchidService();
}

void TQueryRegistry::WriteStateToStderr() const
{
    Impl_->WriteStateToStderr();
}

void TQueryRegistry::SaveState()
{
    Impl_->SaveState();
}

void TQueryRegistry::Start()
{
    Impl_->Start();
}

void TQueryRegistry::Stop()
{
    Impl_->Stop();
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
