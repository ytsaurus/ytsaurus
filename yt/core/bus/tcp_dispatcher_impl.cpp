#include "stdafx.h"
#include "tcp_dispatcher_impl.h"
#include "config.h"
#include "tcp_connection.h"

#include <core/misc/address.h>

#ifndef _win_
    #include <sys/socket.h>
    #include <sys/un.h>
#endif

namespace NYT {
namespace NBus {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = BusLogger;
static auto& Profiler = BusProfiler;
static const int ThreadCount = 8;

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetLocalBusAddress(int port)
{
#ifdef _win_
    UNUSED(port);
    THROW_ERROR_EXCEPTION("Local bus transport is not supported under this platform");
#else
    auto name = Sprintf("yt-local-bus-%d", port);
    sockaddr_un sockAddr;
    memset(&sockAddr, 0, sizeof(sockAddr));
    sockAddr.sun_family = AF_UNIX;
    strncpy(sockAddr.sun_path + 1, ~name, name.length());
    return TNetworkAddress(
        *reinterpret_cast<sockaddr*>(&sockAddr),
        sizeof (sockAddr.sun_family) +
        sizeof (char) +
        name.length());
#endif
}

bool IsLocalServiceAddress(const Stroka& address)
{
#ifdef _linux_
    TStringBuf hostName;
    int port;
    try {
        ParseServiceAddress(address, &hostName, &port);
        return hostName == TAddressResolver::Get()->GetLocalHostName();
    } catch (...) {
        return false;
    }
#else
    // Domain sockets are only supported for Linux.
    UNUSED(address);
    return false;
#endif
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcherInvokerQueue::TTcpDispatcherInvokerQueue(TTcpDispatcherThread* owner)
    : TInvokerQueue(
        &owner->EventCount,
        NProfiling::EmptyTagIds,
        false,
        false)
    , Owner(owner)
{ }

void TTcpDispatcherInvokerQueue::Invoke(const TClosure& callback)
{
    TInvokerQueue::Invoke(callback);
    Owner->CallbackWatcher.send();
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcherThread::TTcpDispatcherThread(const Stroka& threadName)
    : TExecutorThread(
        &EventCount,
        threadName,
        NProfiling::EmptyTagIds,
        false,
        false)
    , Statistics_(ETcpInterfaceType::GetDomainSize())
    , CallbackQueue(New<TTcpDispatcherInvokerQueue>(this))
    , CallbackWatcher(EventLoop)
    , EventWatcher(EventLoop)
{
    // XXX(babenko): VS2013 compat
    Stopped = false;

    CallbackWatcher.set<TTcpDispatcherThread, &TTcpDispatcherThread::OnCallback>(this);
    EventWatcher.set<TTcpDispatcherThread, &TTcpDispatcherThread::OnEvent>(this);

    CallbackWatcher.start();
    EventWatcher.start();
}

void TTcpDispatcherThread::Start()
{
    YCHECK(!Stopped);

    TExecutorThread::Start();
    CallbackQueue->SetThreadId(GetId());
}

void TTcpDispatcherThread::Shutdown()
{
    if (Stopped)
        return;

    Stopped = true;
    
    CallbackWatcher.send();
    
    CallbackQueue->Shutdown();

    TEventEntry entry;
    while (EventQueue.Dequeue(&entry)) { }

    TExecutorThread::Shutdown();
}

const ev::loop_ref& TTcpDispatcherThread::GetEventLoop() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return EventLoop;
}


IInvokerPtr TTcpDispatcherThread::GetInvoker()
{
    VERIFY_THREAD_AFFINITY_ANY();

    return CallbackQueue;
}

EBeginExecuteResult TTcpDispatcherThread::BeginExecute()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    if (Stopped) {
        return EBeginExecuteResult::Terminated;
    }

    EventLoop.run(0);

    if (Stopped) {
        return EBeginExecuteResult::Terminated;
    }

    return CallbackQueue->BeginExecute(&CurrentAction);
}

void TTcpDispatcherThread::EndExecute()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    CallbackQueue->EndExecute(&CurrentAction);
}

void TTcpDispatcherThread::OnCallback(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    EventLoop.break_loop();
}

TAsyncError TTcpDispatcherThread::AsyncRegister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_DEBUG("Object registration enqueued (%s)", ~object->GetLoggingId());

    return BIND(&TTcpDispatcherThread::DoRegister, MakeStrong(this), object)
        .Guarded()
        .AsyncVia(GetInvoker())
        .Run();
}

TAsyncError TTcpDispatcherThread::AsyncUnregister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    LOG_DEBUG("Object unregistration enqueued (%s)", ~object->GetLoggingId());

    return BIND(&TTcpDispatcherThread::DoUnregister, MakeStrong(this), object)
        .Guarded()
        .AsyncVia(GetInvoker())
        .Run();
}

void TTcpDispatcherThread::AsyncPostEvent(TTcpConnectionPtr connection, EConnectionEvent event)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TEventEntry entry(std::move(connection), event);
    EventQueue.Enqueue(entry);
    EventWatcher.send();
}

TTcpDispatcherStatistics& TTcpDispatcherThread::Statistics(ETcpInterfaceType interfaceType)
{
    return Statistics_[static_cast<int>(interfaceType)];
}

void TTcpDispatcherThread::DoRegister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    object->SyncInitialize();
    YCHECK(Objects.insert(object).second);

    LOG_DEBUG("Object registered (%s)", ~object->GetLoggingId());
}

void TTcpDispatcherThread::DoUnregister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    object->SyncFinalize();
    YCHECK(Objects.erase(object) == 1);

    LOG_DEBUG("Object unregistered (%s)", ~object->GetLoggingId());
}

void TTcpDispatcherThread::OnEvent(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TEventEntry entry;
    while (EventQueue.Dequeue(&entry)) {
        entry.Connection->SyncProcessEvent(entry.Event);
    }
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TImpl()
    : ThreadIdGenerator(0)
{
    for (int index = 0; index < ThreadCount; ++index) {
        auto thread = New<TTcpDispatcherThread>(
            Sprintf("Bus:%d", index));
        thread->Start();
        Threads.push_back(thread);
    }
}

TTcpDispatcher::TImpl* TTcpDispatcher::TImpl::Get()
{
    return TTcpDispatcher::Get()->Impl.get();
}

void TTcpDispatcher::TImpl::Shutdown()
{
    for (auto thread : Threads) {
        thread->Shutdown();
    }
}

TTcpDispatcherStatistics TTcpDispatcher::TImpl::GetStatistics(ETcpInterfaceType interfaceType) const
{
    // This is racy but should be OK as an approximation.
    TTcpDispatcherStatistics result;
    for (auto thread : Threads) {
        result += thread->Statistics(interfaceType);
    }
    return result;
}

TTcpDispatcherThreadPtr TTcpDispatcher::TImpl::AllocateThread()
{
    TGuard<TSpinLock> guard(SpinLock);
    size_t index = ThreadIdGenerator.Generate<size_t>() % ThreadCount;
    return Threads[index];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
