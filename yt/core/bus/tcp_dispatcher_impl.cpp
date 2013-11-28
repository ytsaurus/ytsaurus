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

TTcpDispatcherThread::TTcpDispatcherThread(const Stroka& threadName)
    : Statistics_(ETcpInterfaceType::GetDomainSize())
    , ThreadName(threadName)
    , Thread(ThreadFunc, (void*) this)
    , Stopped(false)
    , StopWatcher(EventLoop)
    , RegisterWatcher(EventLoop)
    , UnregisterWatcher(EventLoop)
    , EventWatcher(EventLoop)
{
    StopWatcher.set<TTcpDispatcherThread, &TTcpDispatcherThread::OnStop>(this);
    RegisterWatcher.set<TTcpDispatcherThread, &TTcpDispatcherThread::OnRegister>(this);
    UnregisterWatcher.set<TTcpDispatcherThread, &TTcpDispatcherThread::OnUnregister>(this);
    EventWatcher.set<TTcpDispatcherThread, &TTcpDispatcherThread::OnEvent>(this);

    StopWatcher.start();
    RegisterWatcher.start();
    UnregisterWatcher.start();
    EventWatcher.start();

    Thread.Start();
}

TTcpDispatcherThread::~TTcpDispatcherThread()
{
    Shutdown();
}

void TTcpDispatcherThread::Shutdown()
{
    if (Stopped) {
        return;
    }

    StopWatcher.send();
    Thread.Join();

    {
        TUnregisterEntry entry;
        while (UnregisterQueue.Dequeue(&entry))
        { }
    }

    {
        TRegisterEntry entry;
        while (RegisterQueue.Dequeue(&entry))
        { }
    }

    {
        TEventEntry entry;
        while (EventQueue.Dequeue(&entry))
        { }
    }

    Stopped = true;
}

const ev::loop_ref& TTcpDispatcherThread::GetEventLoop() const
{
    return EventLoop;
}

void* TTcpDispatcherThread::ThreadFunc(void* param)
{
    auto* self = reinterpret_cast<TTcpDispatcherThread*>(param);
    self->ThreadMain();
    return nullptr;
}

void TTcpDispatcherThread::ThreadMain()
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    // NB: never ever use logging or any other YT subsystems here.
    // Bus is always started first to get advantange of the root privileges.

    NConcurrency::SetCurrentThreadName(~ThreadName);
    EventLoop.run(0);
}

void TTcpDispatcherThread::OnStop(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    // NB: No logging here: logging thread may be inactive (e.g. when running with --version).
    EventLoop.break_loop();
}

TAsyncError TTcpDispatcherThread::AsyncRegister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TRegisterEntry entry(object);
    RegisterQueue.Enqueue(entry);
    RegisterWatcher.send();

    LOG_DEBUG("Object registration enqueued (%s)", ~object->GetLoggingId());

    return entry.Promise;
}

TAsyncError TTcpDispatcherThread::AsyncUnregister(IEventLoopObjectPtr object)
{
    VERIFY_THREAD_AFFINITY_ANY();

    TRegisterEntry entry(object);
    UnregisterQueue.Enqueue(entry);
    UnregisterWatcher.send();

    LOG_DEBUG("Object unregistration enqueued (%s)", ~object->GetLoggingId());

    return entry.Promise;
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

void TTcpDispatcherThread::OnRegister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TRegisterEntry entry;
    while (RegisterQueue.Dequeue(&entry)) {
        try {
            LOG_DEBUG("Object registered (%s)", ~entry.Object->GetLoggingId());
            entry.Object->SyncInitialize();
            YCHECK(Objects.insert(entry.Object).second);
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(ex);
        }
    }
}

void TTcpDispatcherThread::OnUnregister(ev::async&, int)
{
    VERIFY_THREAD_AFFINITY(EventLoop);

    TRegisterEntry entry;
    while (UnregisterQueue.Dequeue(&entry)) {
        try {
            LOG_DEBUG("Object unregistered (%s)", ~entry.Object->GetLoggingId());
            YCHECK(Objects.erase(entry.Object) == 1);
            entry.Object->SyncFinalize();
            entry.Promise.Set(TError());
        } catch (const std::exception& ex) {
            entry.Promise.Set(ex);
        }
    }
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
    : Generator(0)
{
    for (int index = 0; index < ThreadCount; ++index) {
        Threads.push_back(New<TTcpDispatcherThread>(
            Sprintf("Bus:%d", index)));
    }
}

TTcpDispatcher::TImpl* TTcpDispatcher::TImpl::Get()
{
    return ~TTcpDispatcher::Get()->Impl;
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
    size_t index = Generator.Generate<size_t>() % ThreadCount;
    return Threads[index];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
