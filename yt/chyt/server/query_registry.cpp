#include "query_registry.h"

#include "query_context.h"
#include "private.h"
#include "config.h"
#include "helpers.h"

#include <yt/yt/core/ytree/ypath_service.h>
#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/core/misc/crash_handler.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <util/generic/bitops.h>

#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;
using namespace NConcurrency;

static const auto& Logger = ClickHouseYtLogger;

////////////////////////////////////////////////////////////////////////////////

//! Class that stores a snapshot of DB::ProcessList::getInfo() and provides
//! convenient accessors to its information.
class TProcessListSnapshot final
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

    const THashMap<TString, DB::ProcessListForUserInfo>& GetUserToProcessListForUserInfo() const
    {
        return UserToProcessListForUserInfo_;
    }

private:
    std::vector<DB::QueryStatusInfo> QueryStatusInfos_;
    THashMap<TQueryId, const DB::QueryStatusInfo*> QueryIdToQueryStatusInfo_;
    THashMap<TString, DB::ProcessListForUserInfo> UserToProcessListForUserInfo_;
};

////////////////////////////////////////////////////////////////////////////////

class TUserProfilingEntry
    : public TRefCounted
{
public:
    std::atomic<int> RunningInitialQueryCount = 0;
    std::atomic<int> RunningSecondaryQueryCount = 0;
    std::atomic<int> HistoricalInitialQueryCount = 0;
    std::atomic<int> HistoricalSecondaryQueryCount = 0;

    TEnumIndexedVector<EQueryPhase, std::atomic<int>> PerPhaseRunningInitialQueryCount;
    TEnumIndexedVector<EQueryPhase, std::atomic<int>> PerPhaseRunningSecondaryQueryCount;

    explicit TUserProfilingEntry(const TProfiler& profiler)
    {
        profiler.AddFuncGauge("/running_initial_query_count", MakeStrong(this), [this] {
            return RunningInitialQueryCount.load();
        });
        profiler.AddFuncGauge("/running_secondary_query_count", MakeStrong(this), [this] {
            return RunningSecondaryQueryCount.load();
        });

        profiler.AddFuncCounter("/historical_initial_query_count", MakeStrong(this), [this] {
            return HistoricalInitialQueryCount.load();
        });
        profiler.AddFuncCounter("/historical_secondary_query_count", MakeStrong(this), [this] {
            return HistoricalSecondaryQueryCount.load();
        });

        for (auto queryPhase : TEnumTraits<EQueryPhase>::GetDomainValues()) {
            if (queryPhase == EQueryPhase::Finish) {
                // There will be no such queries.
                continue;
            }

            profiler
                .WithTag("query_phase", FormatEnum(queryPhase))
                .AddFuncGauge("/running_initial_query_count_per_phase", MakeStrong(this), [this, queryPhase] {
                    return PerPhaseRunningInitialQueryCount[queryPhase].load();
                });

            profiler
                .WithTag("query_phase", FormatEnum(queryPhase))
                .AddFuncGauge("/running_secondary_query_count_per_phase", MakeStrong(this), [this, queryPhase] {
                    return PerPhaseRunningSecondaryQueryCount[queryPhase].load();
                });
        }
    }
};

DECLARE_REFCOUNTED_CLASS(TUserProfilingEntry)
DEFINE_REFCOUNTED_TYPE(TUserProfilingEntry)

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TUserProfilingEntry& userInfo, IYsonConsumer* consumer, const DB::ProcessListForUserInfo* processListForUserInfo)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("running_initial_query_count").Value(userInfo.RunningInitialQueryCount.load())
            .Item("running_secondary_query_count").Value(userInfo.RunningSecondaryQueryCount.load())
            .Item("historical_initial_query_count").Value(userInfo.HistoricalInitialQueryCount.load())
            .Item("historical_secondary_query_count").Value(userInfo.HistoricalSecondaryQueryCount.load())
            .Item("process_list_for_user_info").Value(processListForUserInfo)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

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

        i64 remainingSize = StateAllocationSize_ - StatePointer_ - 1;
        YT_LOG_DEBUG("Building new query registry state (StateSize: %v, RemainingSize: %v)", stateString.size(), remainingSize);
        if (remainingSize < static_cast<i64>(stateString.size())) {
            YT_LOG_DEBUG("Not enough place for new query registry state, moving pointer to the beginning", StatePointer_);
            StatePointer_ = 0;
        }
        remainingSize = StateAllocationSize_ - StatePointer_ - 1;
        if (remainingSize < static_cast<i64>(stateString.size())) {
            YT_LOG_ERROR("Query registry state is too large, it is going to be truncated (StateSize: %v, StateAllocationSize: %v)",
                stateString.size(),
                StateAllocationSize_);
            static const char* truncatedMarker = "...TRUNCATED";
            stateString.resize(StateAllocationSize_ - 13 /* sizeof(truncatedMarker)*/);
            stateString += truncatedMarker;
        }

        YT_VERIFY(StatePointer_ + stateString.size() + 1 <= StateAllocationSize_);

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
    static constexpr i64 StateAllocationSize_ = 128_MB;
    std::array<char, StateAllocationSize_> StateBuffer_;
    i64 StatePointer_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TQueryRegistry::TImpl
    : public NProfiling::ISensorProducer, DB::WithContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, OrchidService);

    TImpl(IInvokerPtr invoker, DB::ContextPtr context, TQueryRegistryConfigPtr config)
        : DB::WithContext(std::move(context))
        , OrchidService_(IYPathService::FromProducer(BIND(&TImpl::BuildYson, MakeWeak(this)))->Via(invoker))
        , Config_(std::move(config))
        , Invoker_(std::move(invoker))
        , QueryRegistryProfiler_(ClickHouseYtProfiler.WithPrefix("/query_registry"))
        , IdlePromise_(MakePromise<void>(TError()))
        , ProcessListSnapshotExecutor_(New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TImpl::UpdateProcessListSnapshot, MakeWeak(this)),
            Config_->ProcessListSnapshotUpdatePeriod))
    {
        TotalDurationTimer_ = QueryRegistryProfiler_.Timer("/total_duration");
        for (auto queryPhase : {EQueryPhase::Preparation, EQueryPhase::Execution}) {
            PhaseDurationTimer_[queryPhase] = QueryRegistryProfiler_.Timer("/phase_duration");
        }

        ClickHouseProfiler
            .WithSparse()
            .AddProducer("", MakeStrong(this));
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
        VERIFY_INVOKER_AFFINITY(Invoker_);

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
        VERIFY_INVOKER_AFFINITY(Invoker_);

        const auto& Logger = queryContext->Logger;

        YT_VERIFY(QueryContexts_.erase(queryContext));

        auto& userProfilingEntry = GetOrCrash(UserToUserProfilingEntry_, queryContext->User);
        switch (queryContext->QueryKind) {
            case EQueryKind::InitialQuery:
                --userProfilingEntry->RunningInitialQueryCount;
                --userProfilingEntry->PerPhaseRunningInitialQueryCount[queryContext->GetQueryPhase()];
                break;
            case EQueryKind::SecondaryQuery:
                --userProfilingEntry->RunningSecondaryQueryCount;
                --userProfilingEntry->PerPhaseRunningSecondaryQueryCount[queryContext->GetQueryPhase()];
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
                --userProfilingEntry->PerPhaseRunningInitialQueryCount[fromPhase];
                ++userProfilingEntry->PerPhaseRunningInitialQueryCount[toPhase];
                break;
            case EQueryKind::SecondaryQuery:
                --userProfilingEntry->PerPhaseRunningSecondaryQueryCount[fromPhase];
                ++userProfilingEntry->PerPhaseRunningSecondaryQueryCount[toPhase];
                break;
            default:
                YT_ABORT();
        }
    }

    void AccountPhaseDuration(EQueryPhase phase, TDuration duration)
    {
        PhaseDurationTimer_[phase].Record(duration);
    }

    void AccountTotalDuration(TDuration duration)
    {
        TotalDurationTimer_.Record(duration);
    }

    size_t GetQueryCount() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return QueryContexts_.size();
    }

    TFuture<void> GetIdleFuture() const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        return IdlePromise_.ToFuture();
    }

    void CollectSensors(NProfiling::ISensorWriter* writer) override
    {
        TProcessListSnapshot snapshot;
        {
            auto guard = Guard(ProcessListSnapshotLock_);
            snapshot = ProcessListSnapshot_;
        }

        for (const auto& [user, processListForUserInfo] : snapshot.GetUserToProcessListForUserInfo()) {
            NProfiling::TWithTagGuard withTagGuard(writer, "user", user);

            writer->AddGauge("/yt/query_registry/memory_usage", processListForUserInfo.memory_usage);
            writer->AddGauge("/yt/query_registry/peak_memory_usage", processListForUserInfo.peak_memory_usage);

            for (const auto& [name, value] : GetBriefProfileCounters(*processListForUserInfo.profile_counters)) {
                writer->AddCounter("/native/user_profile_events/" + name, value);
            }
        }
    }

    void WriteStateToStderr() const
    {
        SignalSafeState_.WriteToStderr();
    }

    void SaveState()
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        TStringStream stream;
        TYsonWriter writer(&stream, EYsonFormat::Pretty);
        BuildYson(&writer);
        auto result = stream.Str();

        SignalSafeState_.SaveState(result);
    }

    void UpdateProcessListSnapshot()
    {
        auto guard = Guard(ProcessListSnapshotLock_);
        ProcessListSnapshot_ = TProcessListSnapshot(getContext()->getProcessList());
    }

private:
    TQueryRegistryConfigPtr Config_;

    IInvokerPtr Invoker_;
    THashSet<TQueryContextPtr> QueryContexts_;

    NProfiling::TProfiler QueryRegistryProfiler_;

    THashMap<TString, TUserProfilingEntryPtr> UserToUserProfilingEntry_;

    TPromise<void> IdlePromise_;

    TSignalSafeState SignalSafeState_;

    TSpinLock ProcessListSnapshotLock_;
    TProcessListSnapshot ProcessListSnapshot_;

    TPeriodicExecutorPtr ProcessListSnapshotExecutor_;

    TEnumIndexedVector<EQueryPhase, NProfiling::TEventTimer> PhaseDurationTimer_;
    NProfiling::TEventTimer TotalDurationTimer_;

    void BuildYson(IYsonConsumer* consumer) const
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        BuildYsonFluently(consumer)
            .BeginMap()
            .DoIf(Config_->SaveRunningQueries, [&] (TFluentMap fluent) {
                fluent.Item("running_queries").DoMapFor(QueryContexts_, [&] (TFluentMap fluent, const TQueryContextPtr queryContext) {
                    const auto& queryId = queryContext->QueryId;
                    fluent.Item(ToString(queryId)).Value(*queryContext, ProcessListSnapshot_.FindQueryStatusInfoByQueryId(queryId));
                });
            })
            .DoIf(Config_->SaveUsers, [&] (TFluentMap fluent) {
                fluent.Item("users").DoMapFor(UserToUserProfilingEntry_, [&] (TFluentMap fluent, const auto& pair) {
                    const auto& [user, userProfilingEntry] = pair;
                    fluent.Item(user).Value(*userProfilingEntry, ProcessListSnapshot_.FindProcessListForUserInfoByUser(user));
                });
            })
            .EndMap();
    }

    TUserProfilingEntry& GetOrRegisterUserProfilingEntry(const TString& user)
    {
        VERIFY_INVOKER_AFFINITY(Invoker_);

        THashMap<TString, TUserProfilingEntryPtr>::insert_ctx ctx;
        auto it = UserToUserProfilingEntry_.find(user, ctx);
        if (it == UserToUserProfilingEntry_.end()) {
            auto profiler = QueryRegistryProfiler_
                .WithTag("user", user)
                .WithSparse();
            it = UserToUserProfilingEntry_.emplace_direct(ctx, user, New<TUserProfilingEntry>(profiler));
        }
        return *it->second;
    }
};

////////////////////////////////////////////////////////////////////////////////

TQueryRegistry::TQueryRegistry(IInvokerPtr invoker, DB::ContextPtr context, TQueryRegistryConfigPtr config)
    : Impl_(New<TImpl>(std::move(invoker), std::move(context), std::move(config)))
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

void TQueryRegistry::AccountPhaseDuration(EQueryPhase phase, TDuration duration)
{
    Impl_->AccountPhaseDuration(phase, duration);
}

void TQueryRegistry::AccountTotalDuration(TDuration duration)
{
    Impl_->AccountTotalDuration(duration);
}

size_t TQueryRegistry::GetQueryCount() const
{
    return Impl_->GetQueryCount();
}

TFuture<void> TQueryRegistry::GetIdleFuture() const
{
    return Impl_->GetIdleFuture();
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
