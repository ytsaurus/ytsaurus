#pragma once

#include "discovery.h"

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryBase
    : public IDiscovery
{
public:
    TDiscoveryBase(
        TDiscoveryBaseConfigPtr config,
        IInvokerPtr invoker,
        NLogging::TLogger logger);

    THashMap<std::string, NYTree::IAttributeDictionaryPtr> List(bool includeBanned = false) const override;
    void Ban(const std::string& name) override;
    void Ban(const std::vector<std::string>& names) override;
    void Unban(const std::string& name) override;
    void Unban(const std::vector<std::string>& names) override;
    TFuture<void> UpdateList(TDuration maxDivergency = TDuration::Zero()) override;
    TFuture<void> StartPolling() override;
    TFuture<void> StopPolling() override;

protected:
    TDiscoveryBaseConfigPtr Config_;
    IInvokerPtr Invoker_;
    NConcurrency::TPeriodicExecutorPtr PeriodicExecutor_;
    THashMap<std::string, NYTree::IAttributeDictionaryPtr> List_;
    THashMap<std::string, TInstant> BannedUntil_;
    TDuration BanTimeout_;
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    const NLogging::TLogger Logger;
    std::optional<std::pair<std::string, NYTree::IAttributeDictionaryPtr>> NameAndAttributes_;
    TFuture<void> ScheduledForceUpdate_;
    TInstant LastUpdate_;

    virtual void DoUpdateList() = 0;
    //! Same as DoUpdateList, but catch and discard all exceptions.
    void DoUpdateListNonThrowing();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
