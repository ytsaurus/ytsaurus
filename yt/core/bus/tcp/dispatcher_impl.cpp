#include "dispatcher_impl.h"
#include "config.h"
#include "connection.h"

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/actions/invoker_util.h>

namespace NYT::NBus {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const auto& Profiler = BusProfiler;

static constexpr auto XferThreadCount = 8;
static constexpr auto ProfilingPeriod = TDuration::Seconds(1);
static constexpr auto LivenessCheckPeriod = TDuration::MilliSeconds(100);
static constexpr auto PerConnectionLivenessChecksPeriod = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

TNetworkAddress GetLocalBusAddress(int port)
{
    auto name = Format("yt-local-bus-%v", port);
    return TNetworkAddress::CreateAbstractUnixDomainSocketAddress(name);
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

const TIntrusivePtr<TTcpDispatcher::TImpl>& TTcpDispatcher::TImpl::Get()
{
    return TTcpDispatcher::Get()->Impl_;
}

void TTcpDispatcher::TImpl::Shutdown()
{
    {
        TGuard guard(PeriodicExecutorsLock_);
        if (ProfilingExecutor_) {
            ProfilingExecutor_->Stop();
        }
        if (LivenessCheckExecutor_) {
            LivenessCheckExecutor_->Stop();
        }
    }

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
        TReaderGuard guard(PollerLock_);
        if (Terminated_) {
            throwAlreadyTerminated();
        }
        if (*poller) {
            return *poller;
        }
    }

    IPollerPtr createdPoller;
    {
        TWriterGuard guard(PollerLock_);
        if (Terminated_) {
            throwAlreadyTerminated();
        }
        if (!*poller) {
            createdPoller = CreateThreadPoolPoller(threadCount, threadNamePrefix);
            *poller = createdPoller;
        }
    }

    StartPeriodicExecutors();

    return *poller;
}

void TTcpDispatcher::TImpl::ShutdownPoller(IPollerPtr* poller)
{
    IPollerPtr swappedPoller;
    {
        TWriterGuard guard(PollerLock_);
        Terminated_ = true;
        std::swap(*poller, swappedPoller);
    }
    if (swappedPoller) {
        swappedPoller->Shutdown();
    }
}

IPollerPtr TTcpDispatcher::TImpl::GetAcceptorPoller()
{
    static const TString ThreadNamePrefix("BusAcceptor");
    return GetOrCreatePoller(&AcceptorPoller_, 1, ThreadNamePrefix);
}

IPollerPtr TTcpDispatcher::TImpl::GetXferPoller()
{
    static const TString ThreadNamePrefix("BusXfer");
    return GetOrCreatePoller(&XferPoller_, XferThreadCount, ThreadNamePrefix);
}

void TTcpDispatcher::TImpl::RegisterConnection(TTcpConnectionPtr connection)
{
    ConnectionsToRegister_.Enqueue(std::move(connection));
}

void TTcpDispatcher::TImpl::StartPeriodicExecutors()
{
    auto invoker = GetXferPoller()->GetInvoker();

    TGuard guard(PeriodicExecutorsLock_);
    if (!ProfilingExecutor_) {
        ProfilingExecutor_ = New<TPeriodicExecutor>(
            invoker,
            BIND(&TImpl::OnProfiling, MakeWeak(this)),
            ProfilingPeriod);
        ProfilingExecutor_->Start();
    }
    if (!LivenessCheckExecutor_) {
        LivenessCheckExecutor_ = New<TPeriodicExecutor>(
            invoker,
            BIND(&TImpl::OnLivenessCheck, MakeWeak(this)),
            ProfilingPeriod);
        LivenessCheckExecutor_->Start();
    }
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

void TTcpDispatcher::TImpl::OnLivenessCheck()
{
    for (auto&& connection : ConnectionsToRegister_.DequeueAll()) {
        ConnectionList_.push_back(std::move(connection));
    }

    if (ConnectionList_.empty()) {
        return;
    }

    i64 connectionsToCheck = std::max(
        static_cast<i64>(ConnectionList_.size()) *
        static_cast<i64>(LivenessCheckPeriod.GetValue()) /
        static_cast<i64>(PerConnectionLivenessChecksPeriod.GetValue()),
        static_cast<i64>(1));
    for (i64 index = 0; index < connectionsToCheck; ++index) {
        auto& weakConnection = ConnectionList_[CurrentConnectionListIndex_];
        if (auto connection = weakConnection.Lock()) {
            connection->CheckLiveness();
            ++CurrentConnectionListIndex_;
        } else {
            std::swap(weakConnection, ConnectionList_.back());
            ConnectionList_.pop_back();
        }
        if (CurrentConnectionListIndex_ >= static_cast<int>(ConnectionList_.size())) {
            CurrentConnectionListIndex_ = 0;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
