#include "query_registry.h"

#include "query_context.h"
#include "private.h"

#include <yt/core/ytree/ypath_service.h>
#include <yt/core/ytree/fluent.h>

#include <yt/core/misc/crash_handler.h>

#include <yt/core/profiling/profile_manager.h>

#include <Interpreters/ProcessList.h>
#include <Interpreters/Context.h>

namespace NYT::NClickHouseServer {

using namespace NProfiling;
using namespace NYTree;
using namespace NYson;

static const auto& Logger = ServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TUserInfo
{
public:
    int RunningInitialQueryCount = 0;
    int RunningSecondaryQueryCount = 0;
    int HistoricalInitialQueryCount = 0;
    int HistoricalSecondaryQueryCount = 0;

    NProfiling::TTagId TagId;

    TUserInfo(const TString& name, DB::Context& globalContext)
        : TagId(TProfileManager::Get()->RegisterTag("user", name))
        , GlobalContext_(&globalContext)
        , Name_(name)
    { }

    i64 GetMemoryUsage() const
    {
        if (auto* processListForUser = TryGetProcessListForUser()) {
            return processListForUser->user_memory_tracker.get();
        }
        return 0;
    }

    i64 GetPeakMemoryUsage() const
    {
        if (auto* processListForUser = TryGetProcessListForUser()) {
            return processListForUser->user_memory_tracker.getPeak();
        }
        return 0;
    }

private:
    DB::Context* GlobalContext_;
    TString Name_;

    DB::ProcessListForUser* TryGetProcessListForUser() const
    {
        return GlobalContext_->getProcessList().getProcessListForUser(Name_);
    }
};

TString ToString(const TUserInfo& userInfo)
{
    return Format(
        "{RI: %v, RS: %v, HI: %v, HS: %v, MU: %v, PMU: %v}",
        userInfo.RunningInitialQueryCount,
        userInfo.RunningSecondaryQueryCount,
        userInfo.HistoricalInitialQueryCount,
        userInfo.HistoricalSecondaryQueryCount,
        userInfo.GetMemoryUsage(),
        userInfo.GetPeakMemoryUsage());
}

void Serialize(const TUserInfo& userInfo, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("running_initial_query_count").Value(userInfo.RunningInitialQueryCount)
            .Item("running_secondary_query_count").Value(userInfo.RunningSecondaryQueryCount)
            .Item("historical_initial_query_count").Value(userInfo.HistoricalInitialQueryCount)
            .Item("historical_secondary_query_count").Value(userInfo.HistoricalSecondaryQueryCount)
            .Item("memory_usage").Value(userInfo.GetMemoryUsage())
            .Item("peak_memory_usage").Value(userInfo.GetPeakMemoryUsage())
        .EndMap();
}

/////////////////////////////////////////////////////////////////////////////

class TQueryRegistry::TImpl
    : public TRefCounted
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NYTree::IYPathServicePtr, OrchidService);

    TImpl(TBootstrap* bootstrap)
        : OrchidService_(IYPathService::FromProducer(BIND(&TImpl::BuildYson, MakeWeak(this))))
        , Bootstrap_(bootstrap)
        , IdlePromise_(MakePromise<void>(TError()))
    {
        memset(&StateBuffer_, 0, sizeof(StateBuffer_));
    }

    void Register(TQueryContext* queryContext)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        const auto& Logger = queryContext->Logger;

        YT_VERIFY(Queries_.insert(queryContext).second);

        THashMap<TString, TUserInfo>::insert_ctx ctx;
        auto it = UserToUserInfo_.find(queryContext->User, ctx);
        if (it == UserToUserInfo_.end()) {
            it = UserToUserInfo_.emplace_direct(
                ctx,
                queryContext->User,
                TUserInfo(queryContext->User, Bootstrap_->GetHost()->GetContext()));
        }
        auto& userInfo = it->second;

        switch (queryContext->QueryKind)
        {
            case EQueryKind::InitialQuery:
                ++userInfo.HistoricalInitialQueryCount;
                ++userInfo.RunningInitialQueryCount;
                break;
            case EQueryKind::SecondaryQuery:
                ++userInfo.HistoricalSecondaryQueryCount;
                ++userInfo.RunningSecondaryQueryCount;
                break;
            default:
                YT_ABORT();
        }

        SaveState();

        YT_LOG_INFO("Query registered (UserInfo: %v)", userInfo);

        if (Queries_.size() == 1) {
            IdlePromise_ = NewPromise<void>();
        }
    }

    void Unregister(TQueryContext* queryContext)
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        const auto& Logger = queryContext->Logger;

        YT_VERIFY(Queries_.erase(queryContext));

        auto it = UserToUserInfo_.find(queryContext->User);
        YT_VERIFY(it != UserToUserInfo_.end());
        auto& userInfo = it->second;
        switch (queryContext->QueryKind)
        {
            case EQueryKind::InitialQuery:
                --userInfo.RunningInitialQueryCount;
                break;
            case EQueryKind::SecondaryQuery:
                --userInfo.RunningSecondaryQueryCount;
                break;
            default:
                YT_ABORT();
        }

        SaveState();

        YT_LOG_INFO("Query unregistered (UserInfo: %v)", userInfo);

        if (Queries_.empty()) {
            IdlePromise_.Set();
        }
    }

    size_t GetQueryCount() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        return Queries_.size();
    }

    TFuture<void> GetIdleFuture() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        return IdlePromise_.ToFuture();
    }

    void OnProfiling() const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        std::lock_guard<std::mutex> guard(Bootstrap_->GetHost()->GetContext().getProcessList().mutex);

        for (const auto& [user, userInfo] : UserToUserInfo_) {
            if (userInfo.RunningInitialQueryCount > 0) {
                ServerProfiler.Enqueue(
                    "/running_initial_query_count",
                    userInfo.RunningInitialQueryCount,
                    EMetricType::Gauge,
                    {userInfo.TagId});
            }

            if (userInfo.RunningSecondaryQueryCount > 0) {
                ServerProfiler.Enqueue(
                    "/running_secondary_query_count",
                    userInfo.RunningSecondaryQueryCount,
                    EMetricType::Gauge,
                    {userInfo.TagId});
            }

            ServerProfiler.Enqueue(
                "/historical_initial_query_count",
                userInfo.HistoricalInitialQueryCount,
                EMetricType::Counter,
                {userInfo.TagId});

            ServerProfiler.Enqueue(
                "/historical_secondary_query_count",
                userInfo.HistoricalSecondaryQueryCount,
                EMetricType::Counter,
                {userInfo.TagId});

            ServerProfiler.Enqueue(
                "/memory_usage",
                userInfo.GetMemoryUsage(),
                EMetricType::Gauge,
                {userInfo.TagId});

            ServerProfiler.Enqueue(
                "/peak_memory_usage",
                userInfo.GetPeakMemoryUsage(),
                EMetricType::Gauge,
                {userInfo.TagId});
        }
    }

    void WriteStateToStderr() const
    {
        int startPosition = StatePointer_;
        int zeroPosition;
        for (zeroPosition = startPosition; StateBuffer_[zeroPosition]; ++zeroPosition);
        WriteToStderr(&StateBuffer_[startPosition], zeroPosition - startPosition);
    }

private:
    TBootstrap* Bootstrap_;
    THashSet<TQueryContext*> Queries_;

    THashMap<TString, TUserInfo> UserToUserInfo_;

    TPromise<void> IdlePromise_;

    static constexpr size_t StateAllocationSize_ = 128_MB;
    std::array<char, StateAllocationSize_> StateBuffer_;
    int StatePointer_ = 0;

    void SaveState()
    {
        YT_LOG_INFO("Saving query registry state (StatePointer: %v)", StatePointer_);
        TStringStream stream;
        TYsonWriter writer(&stream);
        BuildYson(&writer);
        auto result = stream.Str();

        size_t remainingSize = StateAllocationSize_ - StatePointer_ - 1;
        YT_LOG_DEBUG("New query registry state built (StateSize: %v, RemainingSize: %v)", result.size(), remainingSize);
        if (remainingSize < result.size()) {
            YT_LOG_INFO("Not enough place for new query registry state, moving pointer to the beginning", StatePointer_);
            StatePointer_ = 0;
        }
        remainingSize = StateAllocationSize_ - StatePointer_ - 1;
        if (remainingSize < result.size()) {
            YT_LOG_ERROR("Query registry state is too large, it is going to be truncated (StateSize: %v, StateAllocationSize: %v)",
                result.size(),
                StateAllocationSize_);
            static const char* truncatedMarker = "...TRUNCATED";
            result.resize(StateAllocationSize_ - 13 /* sizeof(truncatedMarker)*/);
            result += truncatedMarker;
        }

        strcpy(&StateBuffer_[StatePointer_], result.data());
        StatePointer_ += result.size() + 1;
        YT_LOG_INFO("Query registry state saved (StatePointer: %v)", StatePointer_);
    }

    void BuildYson(IYsonConsumer* consumer) const
    {
        VERIFY_INVOKER_AFFINITY(Bootstrap_->GetControlInvoker());

        std::lock_guard<std::mutex> guard(Bootstrap_->GetHost()->GetContext().getProcessList().mutex);

        BuildYsonFluently(consumer)
            .BeginMap()
            .Item("running_queries").DoMapFor(Queries_, [&] (TFluentMap fluent, const TQueryContext* queryContext) {
                fluent
                    .Item(ToString(queryContext->QueryId)).Value(queryContext);
            })
            .Item("users").DoMapFor(UserToUserInfo_, [&] (TFluentMap fluent, const auto& pair) {
                const auto& [user, userInfo] = pair;
                fluent
                    .Item(user).Value(userInfo);
            })
            .EndMap();
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

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
