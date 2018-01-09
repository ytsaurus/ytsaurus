#pragma once

#include "private.h"
#include "tcp_dispatcher.h"

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/net/address.h>

#include <yt/core/misc/error.h>

#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYT {
namespace NBus {

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

private:
    friend class TTcpDispatcher;

    TImpl();
    DECLARE_NEW_FRIEND();

    void OnProfiling();

    NConcurrency::IPollerPtr GetOrCreatePoller(
        NConcurrency::IPollerPtr* poller,
        int threadCount,
        const TString& threadNamePrefix);
    void ShutdownPoller(NConcurrency::IPollerPtr* poller);

    mutable NConcurrency::TReaderWriterSpinLock SpinLock_;
    bool Terminated_ = false;
    NConcurrency::IPollerPtr AcceptorPoller_;
    NConcurrency::IPollerPtr XferPoller_;

    struct TNetworkStatistics
    {
        NProfiling::TTagId Tag;
        TTcpDispatcherCountersPtr Counters = New<TTcpDispatcherCounters>();
    };

    NConcurrency::TReaderWriterSpinLock StatisticsLock_;
    yhash<TString, TNetworkStatistics> NetworkStatistics_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
