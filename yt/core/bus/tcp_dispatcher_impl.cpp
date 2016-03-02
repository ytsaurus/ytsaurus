#include "tcp_dispatcher_impl.h"
#include "config.h"
#include "tcp_connection.h"

#include <yt/core/misc/address.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/concurrency/periodic_executor.h>

#ifdef _linux_
    #include <sys/socket.h>
    #include <sys/un.h>
#endif

namespace NYT {
namespace NBus {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BusLogger;
static const auto& Profiler = BusProfiler;

static const int ThreadCount = 8;

static const auto ProfilingPeriod = TDuration::MilliSeconds(100);
static const auto CheckPeriod = TDuration::Seconds(15);

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetUnixDomainAddress(const Stroka& name)
{
#ifdef _linux_
    // Abstract unix sockets are supported only on Linux.
    sockaddr_un sockAddr;
    memset(&sockAddr, 0, sizeof(sockAddr));
    sockAddr.sun_family = AF_UNIX;
    strncpy(sockAddr.sun_path + 1, ~name, name.length());
    return TNetworkAddress(
        *reinterpret_cast<sockaddr*>(&sockAddr),
        sizeof (sockAddr.sun_family) +
        sizeof (char) +
        name.length());
#else
    THROW_ERROR_EXCEPTION("Local bus transport is not supported under this platform");
#endif
}

TNetworkAddress GetLocalBusAddress(int port)
{
    auto name = Format("yt-local-bus-%v", port);
    return GetUnixDomainAddress(name);
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
    // Abstract unix sockets (domain sockets) are supported only on Linux.
    UNUSED(address);
    return false;
#endif
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcherThread::TTcpDispatcherThread(const Stroka& threadName)
    : TEVSchedulerThread(threadName, false)
    , CheckExecutor_(New<TPeriodicExecutor>(
        GetInvoker(),
        BIND(&TTcpDispatcherThread::OnCheck, MakeWeak(this)),
        CheckPeriod))
{
    CheckExecutor_->Start();
}

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

TTcpDispatcherStatistics* TTcpDispatcherThread::GetStatistics(ETcpInterfaceType interfaceType)
{
    return &Statistics_[interfaceType];
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

void TTcpDispatcherThread::OnShutdown()
{
    CheckExecutor_->Stop();
    TEVSchedulerThread::OnShutdown();
}

void TTcpDispatcherThread::OnCheck()
{
    for (const auto& object : Objects_) {
        object->SyncCheck();
    }
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TImpl()
{
    ServerThread_ = New<TTcpDispatcherThread>("BusServer");

    for (int index = 0; index < ThreadCount; ++index) {
        ClientThreads_.emplace_back(New<TTcpDispatcherThread>(Format("BusClient:%v", index)));
    }
    
    ProfilingExecutor_ = New<TPeriodicExecutor>(
        ServerThread_->GetInvoker(),
        BIND(&TImpl::OnProfiling, this),
        ProfilingPeriod);
    ProfilingExecutor_->Start();
}

TTcpDispatcher::TImpl* TTcpDispatcher::TImpl::Get()
{
    return TTcpDispatcher::Get()->Impl_.get();
}

void TTcpDispatcher::TImpl::Shutdown()
{
    ProfilingExecutor_->Stop();

    ServerThread_->Shutdown();

    for (const auto& clientThread : ClientThreads_) {
        clientThread->Shutdown();
    }
}

TTcpDispatcherStatistics TTcpDispatcher::TImpl::GetStatistics(ETcpInterfaceType interfaceType) const
{
    // This is racy but should be OK as an approximation.
    auto result = *ServerThread_->GetStatistics(interfaceType);
    for (const auto& clientThread : ClientThreads_) {
        result += *clientThread->GetStatistics(interfaceType);
    }
    return result;
}

TTcpDispatcherThreadPtr TTcpDispatcher::TImpl::GetServerThread()
{
    if (Y_UNLIKELY(!ServerThread_->IsStarted())) {
        ServerThread_->Start();
    }
    return ServerThread_;
}

TTcpDispatcherThreadPtr TTcpDispatcher::TImpl::GetClientThread()
{
    auto index = CurrentClientThreadIndex_++ % ThreadCount;
    auto& thread = ClientThreads_[index];
    if (Y_UNLIKELY(!thread->IsStarted())) {
        thread->Start();
    }
    return thread;
}

void TTcpDispatcher::TImpl::OnProfiling()
{
    for (auto interfaceType : TEnumTraits<ETcpInterfaceType>::GetDomainValues()) {
        auto* profileManager = NProfiling::TProfileManager::Get();
        NProfiling::TTagIdList tagIds{
            profileManager->RegisterTag("interface", interfaceType)
        };

        auto statistics = GetStatistics(interfaceType);

        Profiler.Enqueue("/in_bytes", statistics.InBytes, tagIds);
        Profiler.Enqueue("/in_packets", statistics.InPackets, tagIds);
        Profiler.Enqueue("/out_bytes", statistics.OutBytes, tagIds);
        Profiler.Enqueue("/out_packets", statistics.OutPackets, tagIds);
        Profiler.Enqueue("/pending_out_bytes", statistics.PendingOutBytes, tagIds);
        Profiler.Enqueue("/pending_out_packets", statistics.PendingOutPackets, tagIds);
        Profiler.Enqueue("/client_connections", statistics.ClientConnections, tagIds);
        Profiler.Enqueue("/server_connections", statistics.ServerConnections, tagIds);
        Profiler.Enqueue("/stalled_reads", statistics.StalledReads, tagIds);
        Profiler.Enqueue("/stalled_writes", statistics.StalledWrites, tagIds);
        Profiler.Enqueue("/read_errors", statistics.ReadErrors, tagIds);
        Profiler.Enqueue("/write_errors", statistics.WriteErrors, tagIds);
        Profiler.Enqueue("/encoder_errors", statistics.EncoderErrors, tagIds);
        Profiler.Enqueue("/decoder_errors", statistics.DecoderErrors, tagIds);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
