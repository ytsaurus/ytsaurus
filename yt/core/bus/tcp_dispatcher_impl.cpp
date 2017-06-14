#include "tcp_dispatcher_impl.h"
#include "config.h"
#include "tcp_connection.h"

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/actions/invoker_util.h>

#ifdef _linux_
    #include <sys/socket.h>
    #include <sys/un.h>
#endif

namespace NYT {
namespace NBus {

using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = BusProfiler;

static constexpr auto XferThreadCount = 8;
static constexpr auto ProfilingPeriod = TDuration::MilliSeconds(100);

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetUnixDomainAddress(const TString& name)
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

TTcpDispatcherStatistics TTcpDispatcherCounters::ToStatistics() const
{
    TTcpDispatcherStatistics result;
#define XX(name) result.name = name.load();
    XX(InBytes)
    XX(InPackets)

    XX(OutBytes)
    XX(OutPackets)

    XX(PendingOutPackets)
    XX(PendingOutBytes)

    XX(ClientConnections)
    XX(ServerConnections)

    XX(StalledReads)
    XX(StalledWrites)

    XX(ReadErrors)
    XX(WriteErrors)

    XX(EncoderErrors)
    XX(DecoderErrors)
#undef XX
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TImpl::TImpl()
    : ProfilingExecutor_(New<TPeriodicExecutor>(
        GetSyncInvoker(),
        BIND(&TImpl::OnProfiling, MakeWeak(this)),
        ProfilingPeriod))
{
    auto* profileManager = TProfileManager::Get();
    for (auto interfaceType : TEnumTraits<ETcpInterfaceType>::GetDomainValues()) {
        InterfaceTypeMap_[interfaceType].Tag = profileManager->RegisterTag("interface", interfaceType);
    }

    ProfilingExecutor_->Start();
}

const TIntrusivePtr<TTcpDispatcher::TImpl>& TTcpDispatcher::TImpl::Get()
{
    return TTcpDispatcher::Get()->Impl_;
}

void TTcpDispatcher::TImpl::Shutdown()
{
    ProfilingExecutor_->Stop();
    ShutdownPoller(&AcceptorPoller_);
    ShutdownPoller(&XferPoller_);
}

const TTcpDispatcherCountersPtr& TTcpDispatcher::TImpl::GetCounters(ETcpInterfaceType interfaceType)
{
    return InterfaceTypeMap_[interfaceType].Counters;
}

TTcpDispatcherStatistics TTcpDispatcher::TImpl::GetStatistics(ETcpInterfaceType interfaceType)
{
    return GetCounters(interfaceType)->ToStatistics();
}

IPollerPtr TTcpDispatcher::TImpl::GetOrCreatePoller(
    IPollerPtr* poller,
    int threadCount,
    const TString& threadNamePrefix)
{
    auto throwAlreadyTerminated = [] () {
        THROW_ERROR_EXCEPTION("Bus subsystem is already terminated");
    };
    {
        TReaderGuard guard(SpinLock_);
        if (Terminated_) {
            throwAlreadyTerminated();
        }
        if (*poller) {
            return *poller;
        }
    }
    {
        TWriterGuard guard(SpinLock_);
        if (Terminated_) {
            throwAlreadyTerminated();
        }
        if (!*poller) {
            auto aPoller = CreateThreadPoolPoller(threadCount, threadNamePrefix);
            *poller = aPoller;
        }
        return *poller;
    }
}

void TTcpDispatcher::TImpl::ShutdownPoller(IPollerPtr* poller)
{
    IPollerPtr swappedPoller;
    {
        TWriterGuard guard(SpinLock_);
        Terminated_ = true;
        std::swap(*poller, swappedPoller);
    }
    if (swappedPoller) {
        swappedPoller->Shutdown();
    }
}

IPollerPtr TTcpDispatcher::TImpl::GetAcceptorPoller()
{
    static const TString threadNamePrefix("BusAcceptor");
    return GetOrCreatePoller(&AcceptorPoller_, 1, threadNamePrefix);
}

IPollerPtr TTcpDispatcher::TImpl::GetXferPoller()
{
    static const TString threadNamePrefix("BusXfer");
    return GetOrCreatePoller(&XferPoller_, XferThreadCount, threadNamePrefix);
}

void TTcpDispatcher::TImpl::OnProfiling()
{
    for (auto interfaceType : TEnumTraits<ETcpInterfaceType>::GetDomainValues()) {
        TTagIdList tagIds{
            InterfaceTypeMap_[interfaceType].Tag
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
