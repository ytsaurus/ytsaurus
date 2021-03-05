#pragma once

#include "private.h"
#include "dispatcher.h"

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/library/syncmap/map.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/lock_free.h>

#include <atomic>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

NNet::TNetworkAddress GetLocalBusAddress(int port);
bool IsLocalBusTransportEnabled();

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher::TImpl
    : public NProfiling::ISensorProducer
{
public:
    static const TIntrusivePtr<TImpl>& Get();
    void Shutdown();

    const TTcpDispatcherCountersPtr& GetCounters(const TString& networkName);

    NConcurrency::IPollerPtr GetAcceptorPoller();
    NConcurrency::IPollerPtr GetXferPoller();

    void RegisterConnection(TTcpConnectionPtr connection);

    void ValidateNetworkingNotDisabled(EMessageDirection messageDirection);

    void Collect(NProfiling::ISensorWriter* writer);

private:
    friend class TTcpDispatcher;

    DECLARE_NEW_FRIEND();

    void StartPeriodicExecutors();
    void OnLivenessCheck();

    NConcurrency::IPollerPtr GetOrCreatePoller(
        NConcurrency::IPollerPtr* poller,
        int threadCount,
        const TString& threadNamePrefix);
    void ShutdownPoller(NConcurrency::IPollerPtr* poller);

    void DisableNetworking();

    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, PollerLock_);
    bool Terminated_ = false;
    NConcurrency::IPollerPtr AcceptorPoller_;
    NConcurrency::IPollerPtr XferPoller_;

    TMultipleProducerSingleConsumerLockFreeStack<TWeakPtr<TTcpConnection>> ConnectionsToRegister_;
    std::vector<TWeakPtr<TTcpConnection>> ConnectionList_;
    int CurrentConnectionListIndex_ = 0;

    struct TNetworkStatistics
    {
        TTcpDispatcherCountersPtr Counters = New<TTcpDispatcherCounters>();
    };

    NConcurrency::TSyncMap<TString, TNetworkStatistics> NetworkStatistics_;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, PeriodicExecutorsLock_);
    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
    NConcurrency::TPeriodicExecutorPtr LivenessCheckExecutor_;

    std::atomic<bool> NetworkingDisabled_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
