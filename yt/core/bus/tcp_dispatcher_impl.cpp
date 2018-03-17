#include "tcp_dispatcher_impl.h"
#include "config.h"
#include "tcp_connection.h"

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/actions/invoker_util.h>

namespace NYT {
namespace NBus {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = BusProfiler;

static constexpr auto XferThreadCount = 8;
static constexpr auto ProfilingPeriod = TDuration::Seconds(1);

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetLocalBusAddress(int port)
{
    auto name = Format("yt-local-bus-%v", port);
    return TNetworkAddress::CreateUnixDomainAddress(name);
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

const TTcpDispatcherCountersPtr& TTcpDispatcher::TImpl::GetCounters(const TString& networkName)
{
    TWriterGuard guard(StatisticsLock_);
    auto it = NetworkStatistics_.find(networkName);
    if (it != NetworkStatistics_.end()) {
        return it->second.Counters;
    }

    auto& statistics = NetworkStatistics_[networkName];
    statistics.Tag = TProfileManager::Get()->RegisterTag("network", networkName);
    return statistics.Counters;
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
    TReaderGuard guard(StatisticsLock_);
    for (const auto& network : NetworkStatistics_) {
        TTagIdList tagIds{
            network.second.Tag
        };

        auto statistics = network.second.Counters->ToStatistics();

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
