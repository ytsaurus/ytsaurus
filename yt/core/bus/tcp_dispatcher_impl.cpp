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

TTcpDispatcherThread::TTcpDispatcherThread(const Stroka& threadName)
    : TEVSchedulerThread(threadName, false)
    , Statistics_(ETcpInterfaceType::GetDomainSize())
    , EventWatcher(EventLoop)
{
    EventWatcher.set<TTcpDispatcherThread, &TTcpDispatcherThread::OnEvent>(this);
    EventWatcher.start();
}

void TTcpDispatcherThread::Shutdown()
{
    TEVSchedulerThread::Shutdown();

    TEventEntry entry;
    while (EventQueue.Dequeue(&entry)) { }
}

const ev::loop_ref& TTcpDispatcherThread::GetEventLoop() const
{
    return EventLoop;
}

TAsyncError TTcpDispatcherThread::AsyncRegister(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object registration enqueued (%s)", ~object->GetLoggingId());

    return BIND(&TTcpDispatcherThread::DoRegister, MakeStrong(this), object)
        .Guarded()
        .AsyncVia(GetInvoker())
        .Run();
}

TAsyncError TTcpDispatcherThread::AsyncUnregister(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object unregistration enqueued (%s)", ~object->GetLoggingId());

    return BIND(&TTcpDispatcherThread::DoUnregister, MakeStrong(this), object)
        .Guarded()
        .AsyncVia(GetInvoker())
        .Run();
}

void TTcpDispatcherThread::AsyncPostEvent(TTcpConnectionPtr connection, EConnectionEvent event)
{
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
    object->SyncInitialize();
    YCHECK(Objects.insert(object).second);

    LOG_DEBUG("Object registered (%s)", ~object->GetLoggingId());
}

void TTcpDispatcherThread::DoUnregister(IEventLoopObjectPtr object)
{
    object->SyncFinalize();
    YCHECK(Objects.erase(object) == 1);

    LOG_DEBUG("Object unregistered (%s)", ~object->GetLoggingId());
}

void TTcpDispatcherThread::OnEvent(ev::async&, int)
{
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
