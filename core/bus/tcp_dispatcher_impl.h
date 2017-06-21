#pragma once

#include "private.h"
#include "tcp_dispatcher.h"

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/error.h>

#include <util/thread/lfqueue.h>

#include <atomic>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetUnixDomainAddress(const TString& name);
TNetworkAddress GetLocalBusAddress(int port);
bool IsLocalBusTransportEnabled();

////////////////////////////////////////////////////////////////////////////////

struct TTcpDispatcherCounters
    : public TIntrinsicRefCounted
{
    std::atomic<i64> InBytes = {0};
    std::atomic<i64> InPackets = {0};

    std::atomic<i64> OutBytes = {0};
    std::atomic<i64> OutPackets = {0};

    std::atomic<i64> PendingOutPackets = {0};
    std::atomic<i64> PendingOutBytes = {0};

    std::atomic<int> ClientConnections = {0};
    std::atomic<int> ServerConnections = {0};

    std::atomic<i64> StalledReads = {0};
    std::atomic<i64> StalledWrites = {0};

    std::atomic<i64> ReadErrors = {0};
    std::atomic<i64> WriteErrors = {0};

    std::atomic<i64> EncoderErrors = {0};
    std::atomic<i64> DecoderErrors = {0};

    TTcpDispatcherStatistics ToStatistics() const;
};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherCounters)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher::TImpl
    : public TRefCounted
{
public:
    static const TIntrusivePtr<TImpl>& Get();
    void Shutdown();

    const TTcpDispatcherCountersPtr& GetCounters(ETcpInterfaceType interfaceType);
    TTcpDispatcherStatistics GetStatistics(ETcpInterfaceType interfaceType);

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

    struct TInterfaceInfo
    {
        NProfiling::TTagId Tag;
        TTcpDispatcherCountersPtr Counters = New<TTcpDispatcherCounters>();
    };

    TEnumIndexedVector<TInterfaceInfo, ETcpInterfaceType> InterfaceTypeMap_;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
