#pragma once

#include "private.h"
#include "dispatcher.h"

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/lock_free.h>

#include <atomic>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

NNet::TNetworkAddress GetLocalBusAddress(int port);
bool IsLocalBusTransportEnabled();

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher::TImpl
    : public TRefCounted
{
public:
    static const TIntrusivePtr<TImpl>& Get();
    void Shutdown();

    const TTcpDispatcherCountersPtr& GetCounters(const TString& networkName);

    NConcurrency::IPollerPtr GetAcceptorPoller();
    NConcurrency::IPollerPtr GetXferPoller();

    void RegisterConnection(TTcpConnectionPtr connection);

private:
    friend class TTcpDispatcher;

    DECLARE_NEW_FRIEND();

    void StartPeriodicExecutors();
    void OnProfiling();
    void OnLivenessCheck();

    NConcurrency::IPollerPtr GetOrCreatePoller(
        NConcurrency::IPollerPtr* poller,
        int threadCount,
        const TString& threadNamePrefix);
    void ShutdownPoller(NConcurrency::IPollerPtr* poller);

    mutable NConcurrency::TReaderWriterSpinLock PollerLock_;
    bool Terminated_ = false;
    NConcurrency::IPollerPtr AcceptorPoller_;
    NConcurrency::IPollerPtr XferPoller_;

    TMultipleProducerSingleConsumerLockFreeStack<TWeakPtr<TTcpConnection>> ConnectionsToRegister_;
    std::vector<TWeakPtr<TTcpConnection>> ConnectionList_;
    int CurrentConnectionListIndex_ = 0;

    struct TNetworkStatistics
    {
        NProfiling::TTagId Tag;
        TTcpDispatcherCountersPtr Counters = New<TTcpDispatcherCounters>();
    };

    NConcurrency::TReaderWriterSpinLock StatisticsLock_;
    THashMap<TString, TNetworkStatistics> NetworkStatistics_;

    TSpinLock PeriodicExecutorsLock_;
    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
    NConcurrency::TPeriodicExecutorPtr LivenessCheckExecutor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
