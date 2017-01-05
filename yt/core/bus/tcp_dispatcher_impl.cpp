#include "tcp_dispatcher_impl.h"
#include "config.h"
#include "tcp_connection.h"

#include <yt/core/misc/address.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>

#ifdef _linux_
    #include <sys/socket.h>
    #include <sys/un.h>
#endif

namespace NYT {
namespace NBus {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BusLogger;
static const auto& Profiler = BusProfiler;

static const int DefaultClientThreadCount = 8;

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
    Y_UNREACHABLE();
#endif
}

TNetworkAddress GetLocalBusAddress(int port)
{
    auto name = Format("yt-local-bus-%v", port);
    return GetUnixDomainAddress(name);
}

bool IsLocalBusTransportEnabled()
{
#ifdef _linux_
    return true;
#else
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
    return EventLoop_;
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

void TTcpDispatcherThread::BeforeShutdown()
{
    CheckExecutor_->Stop();
    TEVSchedulerThread::BeforeShutdown();
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
    auto* profileManager = TProfileManager::Get();
    for (auto interfaceType : TEnumTraits<ETcpInterfaceType>::GetDomainValues()) {
        InterfaceTypeToProfilingTag_[interfaceType] = profileManager->RegisterTag("interface", interfaceType);
    }

    auto serverThread = New<TTcpDispatcherThread>("BusServer");
    Threads_.push_back(serverThread);

    SetClientThreadCount(DefaultClientThreadCount);

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        GetServerThread()->GetInvoker(),
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
    TReaderGuard guard(SpinLock_);

    ProfilingExecutor_->Stop();

    for (const auto& thread : Threads_) {
        thread->Shutdown();
    }
}

TTcpDispatcherStatistics TTcpDispatcher::TImpl::GetStatistics(ETcpInterfaceType interfaceType) const
{
    TReaderGuard guard(SpinLock_);

    // This is racy but should be OK as an approximation.
    TTcpDispatcherStatistics result;
    for (const auto& thread : Threads_) {
        result += *thread->GetStatistics(interfaceType);
    }
    return result;
}

int TTcpDispatcher::TImpl::GetServerConnectionCount(ETcpInterfaceType interfaceType) const
{
    TReaderGuard guard(SpinLock_);

    // A variation of GetStatistics optimized for this single parameter.
    // This is, again, racy but should be OK as an approximation.
    int result = 0;
    for (const auto& thread : Threads_) {
        result += thread->GetStatistics(interfaceType)->ServerConnections;
    }
    return result;
}

TTcpDispatcherThreadPtr TTcpDispatcher::TImpl::GetServerThread()
{
    TReaderGuard guard(SpinLock_);

    const auto& thread = Threads_[0];
    if (Y_UNLIKELY(!thread->IsStarted())) {
        thread->Start();
    }
    return thread;
}

TTcpDispatcherThreadPtr TTcpDispatcher::TImpl::GetClientThread()
{
    TReaderGuard guard(SpinLock_);

    auto index = CurrentClientThreadIndex_++ % ClientThreadCount_;
    const auto& thread = Threads_[index + 1];
    if (Y_UNLIKELY(!thread->IsStarted())) {
        thread->Start();
    }
    return thread;
}

void TTcpDispatcher::TImpl::OnProfiling()
{
    for (auto interfaceType : TEnumTraits<ETcpInterfaceType>::GetDomainValues()) {
        TTagIdList tagIds{
            InterfaceTypeToProfilingTag_[interfaceType]
        };

        auto statistics = GetStatistics(interfaceType);

        Profiler.Enqueue("/in_bytes", statistics.InBytes, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/in_packets", statistics.InPackets, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/out_bytes", statistics.OutBytes, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/out_packets", statistics.OutPackets, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/pending_out_bytes", statistics.PendingOutBytes, EMetricType::Gauge, tagIds);
        Profiler.Enqueue("/pending_out_packets", statistics.PendingOutPackets, EMetricType::Gauge, tagIds);
        Profiler.Enqueue("/client_connections", statistics.ClientConnections, EMetricType::Gauge, tagIds);
        Profiler.Enqueue("/server_connections", statistics.ServerConnections, EMetricType::Gauge, tagIds);
        Profiler.Enqueue("/stalled_reads", statistics.StalledReads, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/stalled_writes", statistics.StalledWrites, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/read_errors", statistics.ReadErrors, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/write_errors", statistics.WriteErrors, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/encoder_errors", statistics.EncoderErrors, EMetricType::Counter, tagIds);
        Profiler.Enqueue("/decoder_errors", statistics.DecoderErrors, EMetricType::Counter, tagIds);
    }
}

void TTcpDispatcher::TImpl::SetClientThreadCount(int clientThreadCount)
{
    TWriterGuard guard(SpinLock_);

    if (clientThreadCount <= ClientThreadCount_) {
        Threads_.resize(clientThreadCount + 1);
    } else {
        for (int index = ClientThreadCount_; index < clientThreadCount; ++index) {
            auto clientThread = New<TTcpDispatcherThread>(Format("BusClient:%v", index));
            Threads_.push_back(clientThread);
        }
    }

    ClientThreadCount_ = clientThreadCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
