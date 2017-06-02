#pragma once

#include "private.h"
#include "tcp_dispatcher.h"

#include <yt/core/concurrency/ev_scheduler_thread.h>
#include <yt/core/concurrency/event_count.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/address.h>
#include <yt/core/misc/error.h>

#include <util/thread/lfqueue.h>

#include <yt/contrib/libev/ev++.h>

#include <atomic>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetUnixDomainAddress(const TString& name);
TNetworkAddress GetLocalBusAddress(int port);
bool IsLocalBusTransportEnabled();

////////////////////////////////////////////////////////////////////////////////

struct IEventLoopObject
    : public virtual TRefCounted
{
    virtual void SyncInitialize() = 0;
    virtual void SyncFinalize() = 0;
    virtual void SyncCheck() = 0;
    virtual TString GetLoggingId() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IEventLoopObject)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcherThread
    : public NConcurrency::TEVSchedulerThread
{
public:
    explicit TTcpDispatcherThread(const TString& threadName);

    const ev::loop_ref& GetEventLoop() const;

    TFuture<void> AsyncRegister(IEventLoopObjectPtr object);
    TFuture<void> AsyncUnregister(IEventLoopObjectPtr object);

    TTcpDispatcherStatistics* GetStatistics(ETcpInterfaceType interfaceType);

private:
    const NConcurrency::TPeriodicExecutorPtr CheckExecutor_;

    TEnumIndexedVector<TTcpDispatcherStatistics, ETcpInterfaceType> Statistics_;
    yhash_set<IEventLoopObjectPtr> Objects_;

    void DoRegister(IEventLoopObjectPtr object);
    void DoUnregister(IEventLoopObjectPtr object);


    virtual void BeforeShutdown() override;

    void OnCheck();

};

DEFINE_REFCOUNTED_TYPE(TTcpDispatcherThread)

////////////////////////////////////////////////////////////////////////////////

class TTcpDispatcher::TImpl
{
public:
    static TImpl* Get();

    void Shutdown();

    TTcpDispatcherStatistics GetStatistics(ETcpInterfaceType interfaceType) const;
    int GetServerConnectionCount(ETcpInterfaceType interfaceType) const;

    TTcpDispatcherThreadPtr GetServerThread();
    TTcpDispatcherThreadPtr GetClientThread();

    void SetClientThreadCount(int clientThreadCount);

private:
    friend class TTcpDispatcher;

    TImpl();
    void OnProfiling();

    TEnumIndexedVector<NProfiling::TTagId, ETcpInterfaceType> InterfaceTypeToProfilingTag_;

    // Server thread + all client threads.
    std::vector<TTcpDispatcherThreadPtr> Threads_;
    unsigned int CurrentClientThreadIndex_ = 0;
    int ClientThreadCount_ = 0;

    NConcurrency::TPeriodicExecutorPtr ProfilingExecutor_;

    NConcurrency::TReaderWriterSpinLock SpinLock_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
