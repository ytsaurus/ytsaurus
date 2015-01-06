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

static const auto& Logger = BusLogger;
static const int ThreadCount = 8;

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetLocalBusAddress(int port)
{
#ifdef _win_
    UNUSED(port);
    THROW_ERROR_EXCEPTION("Local bus transport is not supported under this platform");
#else
    auto name = Format("yt-local-bus-%v", port);
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
{ }

const ev::loop_ref& TTcpDispatcherThread::GetEventLoop() const
{
    return EventLoop;
}

TFuture<void> TTcpDispatcherThread::AsyncRegister(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object registration enqueued (%v)", object->GetLoggingId());

    return BIND(&TTcpDispatcherThread::DoRegister, MakeStrong(this), object)
        .AsyncVia(GetInvoker())
        .Run();
}

TFuture<void> TTcpDispatcherThread::AsyncUnregister(IEventLoopObjectPtr object)
{
    LOG_DEBUG("Object unregistration enqueued (%v)", object->GetLoggingId());

    return BIND(&TTcpDispatcherThread::DoUnregister, MakeStrong(this), object)
        .AsyncVia(GetInvoker())
        .Run();
}

TTcpDispatcherStatistics& TTcpDispatcherThread::Statistics(ETcpInterfaceType interfaceType)
{
    return Statistics_[interfaceType];
}

void TTcpDispatcherThread::DoRegister(IEventLoopObjectPtr object)
{
    object->SyncInitialize();
    YCHECK(Objects_.insert(object).second);

    LOG_DEBUG("Object registered (%v)", object->GetLoggingId());
}

void TTcpDispatcherThread::DoUnregister(IEventLoopObjectPtr object)
{
    object->SyncFinalize();
    YCHECK(Objects_.erase(object) == 1);

    LOG_DEBUG("Object unregistered (%v)", object->GetLoggingId());
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TImpl()
    : ThreadIdGenerator_(0)
{
    for (int index = 0; index < ThreadCount; ++index) {
        auto thread = New<TTcpDispatcherThread>(Format("Bus:%v", index));
        thread->Start();
        Threads_.push_back(thread);
    }
}

TTcpDispatcher::TImpl* TTcpDispatcher::TImpl::Get()
{
    return TTcpDispatcher::Get()->Impl.get();
}

void TTcpDispatcher::TImpl::Shutdown()
{
    for (auto thread : Threads_) {
        thread->Shutdown();
    }
}

TTcpDispatcherStatistics TTcpDispatcher::TImpl::GetStatistics(ETcpInterfaceType interfaceType) const
{
    // This is racy but should be OK as an approximation.
    TTcpDispatcherStatistics result;
    for (auto thread : Threads_) {
        result += thread->Statistics(interfaceType);
    }
    return result;
}

TTcpDispatcherThreadPtr TTcpDispatcher::TImpl::AllocateThread()
{
    TGuard<TSpinLock> guard(SpinLock_);
    size_t index = ThreadIdGenerator_.Generate<size_t>() % ThreadCount;
    return Threads_[index];
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
