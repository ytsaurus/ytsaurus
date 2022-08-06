#include "dispatcher_impl.h"
#include "config.h"
#include "connection.h"

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/thread_pool_poller.h>

#include <yt/yt/core/actions/invoker_util.h>

#include <yt/yt/library/profiling/producer.h>

namespace NYT::NBus {

using namespace NConcurrency;
using namespace NProfiling;
using namespace NNet;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BusLogger;

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

    XX(Retransmits)

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

const TTcpDispatcherCountersPtr& TTcpDispatcher::TImpl::GetCounters(const TString& networkName)
{
    auto [statistics, ok] = NetworkStatistics_.FindOrInsert(networkName, [] {
        return TNetworkStatistics{};
    });

    return statistics->Counters;
}

IPollerPtr TTcpDispatcher::TImpl::GetOrCreatePoller(
    IPollerPtr* pollerPtr,
    bool isXfer,
    const TString& threadNamePrefix)
{
    {
        auto guard = ReaderGuard(PollerLock_);
        if (*pollerPtr) {
            return *pollerPtr;
        }
    }

    IPollerPtr poller;
    {
        auto guard = WriterGuard(PollerLock_);
        if (!*pollerPtr) {
            *pollerPtr = CreateThreadPoolPoller(isXfer ? Config_->ThreadPoolSize : 1, threadNamePrefix);
        }
        poller = *pollerPtr;
    }

    StartPeriodicExecutors();

    return poller;
}

void TTcpDispatcher::TImpl::DisableNetworking()
{
    NetworkingDisabled_.store(true);
}

bool TTcpDispatcher::TImpl::IsNetworkingDisabled()
{
    return NetworkingDisabled_.load();
}

const TString& TTcpDispatcher::TImpl::GetNetworkNameForAddress(const TNetworkAddress& address)
{
    if (address.IsUnix()) {
        return LocalNetworkName;
    }

    if (!address.IsIP6()) {
        return DefaultNetworkName;
    }

    auto ip6Address = address.ToIP6Address();

    {
        auto guard = ReaderGuard(NetworksLock_);
        for (const auto& [networkAddress, networkName] : Networks_) {
            if (networkAddress.Contains(ip6Address)) {
                return networkName;
            }
        }
    }

    return DefaultNetworkName;
}

void TTcpDispatcher::TImpl::ValidateNetworkingNotDisabled(EMessageDirection messageDirection) const
{
    if (Y_UNLIKELY(NetworkingDisabled_.load())) {
        YT_LOG_FATAL("Networking is disabled with global switch, %lv message detected",
            messageDirection);
    }
}

IPollerPtr TTcpDispatcher::TImpl::GetAcceptorPoller()
{
    static const TString ThreadNamePrefix("BusAcpt");
    return GetOrCreatePoller(&AcceptorPoller_, false, ThreadNamePrefix);
}

IPollerPtr TTcpDispatcher::TImpl::GetXferPoller()
{
    static const TString ThreadNamePrefix("BusXfer");
    return GetOrCreatePoller(&XferPoller_, true, ThreadNamePrefix);
}

void TTcpDispatcher::TImpl::Configure(const TTcpDispatcherConfigPtr& config)
{
    {
        auto guard = WriterGuard(PollerLock_);

        Config_ = config;

        if (XferPoller_) {
            XferPoller_->Reconfigure(Config_->ThreadPoolSize);
        }
    }

    {
        auto guard = WriterGuard(NetworksLock_);

        Networks_.clear();

        for (const auto& [networkName, networkAddresses] : config->Networks) {
            for (const auto& prefix : networkAddresses) {
                Networks_.emplace_back(prefix, networkName);
            }
        }

        // Put more specific networks first in match order.
        std::sort(Networks_.begin(), Networks_.end(), [] (const auto& lhs, const auto& rhs) {
            return lhs.first.GetMaskSize() > rhs.first.GetMaskSize();
        });
    }
}

void TTcpDispatcher::TImpl::RegisterConnection(TTcpConnectionPtr connection)
{
    ConnectionsToRegister_.Enqueue(std::move(connection));
}

void TTcpDispatcher::TImpl::StartPeriodicExecutors()
{
    auto poller = GetXferPoller();
    auto invoker = poller->GetInvoker();

    auto guard = Guard(PeriodicExecutorsLock_);
    if (!LivenessCheckExecutor_) {
        LivenessCheckExecutor_ = New<TPeriodicExecutor>(
            invoker,
            BIND(&TImpl::OnLivenessCheck, MakeWeak(this)),
            LivenessCheckPeriod);
        LivenessCheckExecutor_->Start();
    }
}

void TTcpDispatcher::TImpl::CollectSensors(ISensorWriter* writer)
{
    NetworkStatistics_.IterateReadOnly([&] (const auto& name, const auto& statistics) {
        auto counters = statistics.Counters->ToStatistics();
        TWithTagGuard tagGuard(writer, "network", name);
        writer->AddCounter("/in_bytes", counters.InBytes);
        writer->AddCounter("/in_packets", counters.InPackets);
        writer->AddCounter("/out_bytes", counters.OutBytes);
        writer->AddCounter("/out_packets", counters.OutPackets);
        writer->AddGauge("/pending_out_bytes", counters.PendingOutBytes);
        writer->AddGauge("/pending_out_packets", counters.PendingOutPackets);
        writer->AddGauge("/client_connections", counters.ClientConnections);
        writer->AddGauge("/server_connections", counters.ServerConnections);
        writer->AddCounter("/stalled_reads", counters.StalledReads);
        writer->AddCounter("/stalled_writes", counters.StalledWrites);
        writer->AddCounter("/read_errors", counters.ReadErrors);
        writer->AddCounter("/write_errors", counters.WriteErrors);
        writer->AddCounter("/tcp_retransmits", counters.Retransmits);
        writer->AddCounter("/encoder_errors", counters.EncoderErrors);
        writer->AddCounter("/decoder_errors", counters.DecoderErrors);
    });


    TTcpDispatcherConfigPtr config;
    {
        auto guard = ReaderGuard(PollerLock_);
        config = Config_;
    }

    if (config->NetworkBandwidth) {
        writer->AddGauge("/network_bandwidth_limit", *config->NetworkBandwidth);
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
