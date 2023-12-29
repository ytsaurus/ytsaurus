#include "discovery_base.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NClickHouseServer {

using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TDiscoveryBase::TDiscoveryBase(
    TDiscoveryBaseConfigPtr config,
    IInvokerPtr invoker,
    TLogger logger)
    : Config_(std::move(config))
    , Invoker_(std::move(invoker))
    , PeriodicExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TDiscoveryBase::DoUpdateListNonThrowing, MakeWeak(this)),
        Config_->UpdatePeriod))
    , BanTimeout_(std::move(Config_->BanTimeout))
    , Logger(logger.WithTag("GroupId: %v", Config_->GroupId))
{ }

THashMap<TString, IAttributeDictionaryPtr> TDiscoveryBase::List(bool includeBanned) const
{
    THashMap<TString, IAttributeDictionaryPtr> result;
    THashMap<TString, TInstant> bannedUntil;
    decltype(NameAndAttributes_) nameAndAttributes;
    {
        auto guard = ReaderGuard(Lock_);
        result = List_;
        bannedUntil = BannedUntil_;
        nameAndAttributes = NameAndAttributes_;
    }
    auto now = TInstant::Now();
    if (nameAndAttributes) {
        result.insert(*nameAndAttributes);
    }
    if (!includeBanned) {
        for (auto it = result.begin(); it != result.end();) {
            auto banIt = bannedUntil.find(it->first);
            if (banIt != bannedUntil.end() && now < banIt->second) {
                result.erase(it++);
            } else {
                ++it;
            }
        }
    }
    return result;
}

void TDiscoveryBase::Ban(const TString& name)
{
    Ban(std::vector{name});
}

void TDiscoveryBase::Ban(const std::vector<TString>& names)
{
    if (names.empty()) {
        return;
    }
    auto guard = WriterGuard(Lock_);
    auto banDeadline = TInstant::Now() + BanTimeout_;
    for (const auto& name : names) {
        BannedUntil_[name] = banDeadline;
    }
    YT_LOG_INFO("Participants banned (Names: %v, Until: %v)", names, banDeadline);
}

void TDiscoveryBase::Unban(const TString& name)
{
    Unban(std::vector{name});
}

void TDiscoveryBase::Unban(const::std::vector<TString>& names)
{
    if (names.empty()) {
        return;
    }
    auto guard = WriterGuard(Lock_);
    for (const auto& name : names) {
        if (auto it = BannedUntil_.find(name); it != BannedUntil_.end()) {
            BannedUntil_.erase(it);
            YT_LOG_INFO("Participant unbanned (Name: %v)", name);
        }
    }
}

TFuture<void> TDiscoveryBase::UpdateList(TDuration maxDivergency)
{
    auto guard = WriterGuard(Lock_);
    if (LastUpdate_ + maxDivergency >= TInstant::Now()) {
        return VoidFuture;
    }
    if (!ScheduledForceUpdate_ || ScheduledForceUpdate_.IsSet()) {
        ScheduledForceUpdate_ = BIND(&TDiscoveryBase::DoUpdateList, MakeStrong(this))
            .AsyncVia(Invoker_)
            .Run();
        YT_LOG_DEBUG("Force update scheduled");
    }
    return ScheduledForceUpdate_;
}

TFuture<void> TDiscoveryBase::StartPolling()
{
    PeriodicExecutor_->Start();
    return PeriodicExecutor_->GetExecutedEvent();
}

TFuture<void> TDiscoveryBase::StopPolling()
{
    return PeriodicExecutor_->Stop();
}

void TDiscoveryBase::DoUpdateListNonThrowing()
{
    try {
        DoUpdateList();
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to update discovery");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
