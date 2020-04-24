#include "query_registry.h"

#include "query_context.h"
#include "private.h"
#include "config.h"
#include "helpers.h"

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

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

//! Class that stores a snapshot of DB::ProcessList::getInfo() and provides
//! convenient accessors to its information.
class TProcessListSnapshot
{
public:
    TProcessListSnapshot(const DB::ProcessList& processList)
    {
        auto queryStatusInfos = processList.getInfo(true, true, true);
        // NB: reservation is important for stored pointers not to invalidate.
        QueryStatusInfos_.reserve(queryStatusInfos.size());
        for (auto& queryStatusInfo : queryStatusInfos) {
            const auto& queryIdString = queryStatusInfo.client_info.current_query_id;
            const auto& user = queryStatusInfo.client_info.current_user;
            TQueryId queryId;
            if (!TQueryId::FromString(queryIdString, &queryId)) {
                YT_LOG_DEBUG("Process list contains query without proper YT query id, skipping it in query registry "
                    "(QueryId: %v, User: %v)",
                    queryIdString,
                    user);
            }
            QueryStatusInfos_.emplace_back(std::move(queryStatusInfo));
            QueryIdToQueryStatusInfo_[queryId] = &QueryStatusInfos_.back();
        }

        auto userToProcessListForUserInfo = processList.getUserInfo(true);
        UserToProcessListForUserInfo_.insert(userToProcessListForUserInfo.begin(), userToProcessListForUserInfo.end());

        YT_LOG_DEBUG("Process list snapshot built (QueryCount: %v, UserCount: %v)", QueryStatusInfos_.size(), UserToProcessListForUserInfo_.size());
    }

    TProcessListSnapshot() = default;

    const DB::QueryStatusInfo* FindQueryStatusInfoByQueryId(const TQueryId& queryId) const
    {
        auto it = QueryIdToQueryStatusInfo_.find(queryId);
        if (it != QueryIdToQueryStatusInfo_.end()) {
            return it->second;
        }
        return nullptr;
    }

    const DB::ProcessListForUserInfo* FindProcessListForUserInfoByUser(const TString& user) const
    {
        auto it = UserToProcessListForUserInfo_.find(user);
        if (it != UserToProcessListForUserInfo_.end()) {
            return &it->second;
        }
        return nullptr;
    }

private:
    std::vector<DB::QueryStatusInfo> QueryStatusInfos_;
    THashMap<TQueryId, const DB::QueryStatusInfo*> QueryIdToQueryStatusInfo_;
    THashMap<TString, DB::ProcessListForUserInfo> UserToProcessListForUserInfo_;
};

////////////////////////////////////////////////////////////////////////////////

class TUserProfilingEntry
{
public:
    int RunningInitialQueryCount = 0;
    int RunningSecondaryQueryCount = 0;
    int HistoricalInitialQueryCount = 0;
    int HistoricalSecondaryQueryCount = 0;

    TEnumIndexedVector<EQueryPhase, int> PerPhaseRunningInitialQueryCount;
    TEnumIndexedVector<EQueryPhase, int> PerPhaseRunningSecondaryQueryCount;

    NProfiling::TTagId TagId;

    explicit TUserProfilingEntry(TTagId tagId, const TString& name)
        : TagId(tagId)
        , Name_(name)
    { }

private:
    TString Name_;
};

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TUserProfilingEntry& userInfo, IYsonConsumer* consumer, const DB::ProcessListForUserInfo* processListForUserInfo)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("running_initial_query_count").Value(userInfo.RunningInitialQueryCount)
            .Item("running_secondary_query_count").Value(userInfo.RunningSecondaryQueryCount)
            .Item("historical_initial_query_count").Value(userInfo.HistoricalInitialQueryCount)
            .Item("historical_secondary_query_count").Value(userInfo.HistoricalSecondaryQueryCount)
            .Item("process_list_for_user_info").Value(processListForUserInfo)
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
            YT_LOG_DEBUG("Not enough place for new query registry state, moving pointer to the beginning", StatePointer_);
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
        , UserTagCache_("user")
        , IdlePromise_(MakePromise<void>(TError()))
        , ProcessListSnapshotExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetControlInvoker(),
            BIND(&TImpl::UpdateProcessListSnapshot, MakeWeak(this)),
            Bootstrap_->GetConfig()->ProcessListSnapshotUpdatePeriod))
        , QueryRegistryProfiler_(ClickHouseYtProfiler.AppendPath("/query_registry"))
    {
        for (const auto& queryPhase : TEnumTraits<EQueryPhase>::GetDomainValues()) {
            QueryPhaseToProfilingTagId_[queryPhase] = NProfiling::TProfileManager::Get()->RegisterTag("query_phase", FormatEnum(queryPhase));
        }
    }

    void Start()
    {
        ProcessListSnapshotExecutor_->Start();
    }

    void Stop()
    {
        WaitFor(ProcessListSnapshotExecutor_->Stop())
            .ThrowOnError();
    }

    void Register(TQueryContextPtr queryContext)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        const auto& Logger = queryContext->Logger;

        YT_VERIFY(QueryContexts_.insert(queryContext.Get()).second);

        auto& userProfilingEntry = GetOrRegisterUserProfilingEntry(queryContext->User);
        switch (queryContext->QueryKind) {
            case EQueryKind::InitialQuery:
                ++userProfilingEntry.HistoricalInitialQueryCount;
                ++userProfilingEntry.RunningInitialQueryCount;
                ++userProfilingEntry.PerPhaseRunningInitialQueryCount[queryContext->GetQueryPhase()];
                break;
            case EQueryKind::SecondaryQuery:
                ++userProfilingEntry.HistoricalSecondaryQueryCount;
                ++userProfilingEntry.RunningSecondaryQueryCount;
                ++userProfilingEntry.PerPhaseRunningSecondaryQueryCount[queryContext->GetQueryPhase()];
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

    void Unregister(TQueryContextPtr queryContext)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        const auto& Logger = queryContext->Logger;

        YT_VERIFY(QueryContexts_.erase(queryContext));

        auto& userProfilingEntry = GetOrCrash(UserToUserProfilingEntry_, queryContext->User);
        switch (queryContext->QueryKind) {
            case EQueryKind::InitialQuery:
                --userProfilingEntry.RunningInitialQueryCount;
                --userProfilingEntry.PerPhaseRunningInitialQueryCount[queryContext->GetQueryPhase()];
                break;
            case EQueryKind::SecondaryQuery:
                --userProfilingEntry.RunningSecondaryQueryCount;
                --userProfilingEntry.PerPhaseRunningSecondaryQueryCount[queryContext->GetQueryPhase()];
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

    void AccountPhaseCounter(TQueryContextPtr queryContext, EQueryPhase fromPhase, EQueryPhase toPhase)
    {
        auto& userProfilingEntry = GetOrCrash(UserToUserProfilingEntry_, queryContext->User);

        switch (queryContext->QueryKind) {
            case EQueryKind::InitialQuery:
                --userProfilingEntry.PerPhaseRunningInitialQueryCount[fromPhase];
                ++userProfilingEntry.PerPhaseRunningInitialQueryCount[toPhase];
                break;
            case EQueryKind::SecondaryQuery:
                --userProfilingEntry.PerPhaseRunningSecondaryQueryCount[fromPhase];
                ++userProfilingEntry.PerPhaseRunningSecondaryQueryCount[toPhase];
                break;
            default:
                YT_ABORT();
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
            QueryRegistryProfiler_.Enqueue(
                "/running_initial_query_count",
                userProfilingInfo.RunningInitialQueryCount,
                EMetricType::Gauge,
                {userProfilingInfo.TagId});

            QueryRegistryProfiler_.Enqueue(
                "/running_secondary_query_count",
                userProfilingInfo.RunningSecondaryQueryCount,
                EMetricType::Gauge,
                {userProfilingInfo.TagId});

            for (const auto& queryPhase : TEnumTraits<EQueryPhase>::GetDomainValues()) {
                if (queryPhase == EQueryPhase::Finish) {
                    // There will be no such queries.
                    continue;
                }

                QueryRegistryProfiler_.Enqueue(
                    "/running_initial_query_count_per_phase",
                    userProfilingInfo.PerPhaseRunningInitialQueryCount[queryPhase],
                    EMetricType::Gauge,
                    {userProfilingInfo.TagId, QueryPhaseToProfilingTagId_[queryPhase]});

                QueryRegistryProfiler_.Enqueue(
                    "/running_secondary_query_count_per_phase",
                    userProfilingInfo.PerPhaseRunningSecondaryQueryCount[queryPhase],
                    EMetricType::Gauge,
                    {userProfilingInfo.TagId, QueryPhaseToProfilingTagId_[queryPhase]});
            }

            QueryRegistryProfiler_.Enqueue(
                "/historical_initial_query_count",
                userProfilingInfo.HistoricalInitialQueryCount,
                EMetricType::Counter,
                {userProfilingInfo.TagId});

            QueryRegistryProfiler_.Enqueue(
                "/historical_secondary_query_count",
                userProfilingInfo.HistoricalSecondaryQueryCount,
                EMetricType::Counter,
                {userProfilingInfo.TagId});

            if (const auto* processListForUserInfo = ProcessListSnapshot_.FindProcessListForUserInfoByUser(user)) {
                QueryRegistryProfiler_.Enqueue(
                    "/memory_usage",
                    processListForUserInfo->memory_usage,
                    EMetricType::Gauge,
                    {userProfilingInfo.TagId});

                QueryRegistryProfiler_.Enqueue(
                    "/peak_memory_usage",
                    processListForUserInfo->peak_memory_usage,
                    EMetricType::Gauge,
                    {userProfilingInfo.TagId});

                for (const auto& [name, value] : GetBriefProfileCounters(*processListForUserInfo->profile_counters)) {
                    ClickHouseNativeProfiler.Enqueue(
                        "/user_profile_events/" + name,
                        value,
                        EMetricType::Counter,
                        {userProfilingInfo.TagId});
                }
            }
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

    NProfiling::TTagId GetUserProfilingTag(const TString& user)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return UserTagCache_.GetTag(user);
    }

private:
    TBootstrap* Bootstrap_;
    THashSet<TQueryContextPtr> QueryContexts_;

    TTagCache<TString> UserTagCache_;
    THashMap<TString, TUserProfilingEntry> UserToUserProfilingEntry_;

    TPromise<void> IdlePromise_;

    TSignalSafeState SignalSafeState_;

    TProcessListSnapshot ProcessListSnapshot_;

    TPeriodicExecutorPtr ProcessListSnapshotExecutor_;

    TEnumIndexedVector<EQueryPhase, NProfiling::TTagId> QueryPhaseToProfilingTagId_;

    TProfiler QueryRegistryProfiler_;

    void BuildYson(IYsonConsumer* consumer) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        BuildYsonFluently(consumer)
            .BeginMap()
            .Item("running_queries").DoMapFor(QueryContexts_, [&] (TFluentMap fluent, const TQueryContextPtr queryContext) {
                const auto& queryId = queryContext->QueryId;
                fluent
                    .Item(ToString(queryId)).Value(*queryContext, ProcessListSnapshot_.FindQueryStatusInfoByQueryId(queryId));
            })
            .Item("users").DoMapFor(UserToUserProfilingEntry_, [&] (TFluentMap fluent, const auto& pair) {
                const auto& [user, userProfilingEntry] = pair;
                fluent
                    .Item(user).Value(userProfilingEntry, ProcessListSnapshot_.FindProcessListForUserInfoByUser(user));
            })
            .EndMap();
    }

    TUserProfilingEntry& GetOrRegisterUserProfilingEntry(const TString& user)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        THashMap<TString, TUserProfilingEntry>::insert_ctx ctx;
        auto it = UserToUserProfilingEntry_.find(user, ctx);
        if (it == UserToUserProfilingEntry_.end()) {
            auto tagId = GetUserProfilingTag(user);
            it = UserToUserProfilingEntry_.emplace_direct(ctx, user, TUserProfilingEntry(tagId, user));
        }
        return it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

TQueryRegistry::TQueryRegistry(NYT::NClickHouseServer::TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TQueryRegistry::~TQueryRegistry()
{ }

void TQueryRegistry::Register(TQueryContextPtr queryContext)
{
    Impl_->Register(std::move(queryContext));
}

void TQueryRegistry::Unregister(TQueryContextPtr queryContext)
{
    Impl_->Unregister(std::move(queryContext));
}

void TQueryRegistry::AccountPhaseCounter(TQueryContextPtr queryContext, EQueryPhase fromPhase, EQueryPhase toPhase)
{
    Impl_->AccountPhaseCounter(std::move(queryContext), fromPhase, toPhase);
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

NProfiling::TTagId TQueryRegistry::GetUserProfilingTag(const TString& user)
{
    return Impl_->GetUserProfilingTag(user);
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
