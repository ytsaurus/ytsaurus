#include "dispatcher.h"
#include "config.h"

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/thread_pool.h>
#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/singleton.h>
#include <yt/core/misc/shutdown.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NBus;


////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
    {
        NetworkNames_.push_back(DefaultNetworkName);
        for (auto band : TEnumTraits<EMultiplexingBand>::GetDomainValues()) {
            auto& bandDescriptor = BandToDescriptor_[band];
            bandDescriptor.NetworkIdToTosLevel.resize(NetworkNames_.size(), bandDescriptor.DefaultTosLevel);
        }
    }

    void Configure(const TDispatcherConfigPtr& config)
    {
        HeavyPool_->Configure(config->HeavyPoolSize);
        TWriterGuard guard(SpinLock_);

        for (auto band : TEnumTraits<EMultiplexingBand>::GetDomainValues()) {
            const auto& bandConfig = config->MultiplexingBands[band];
            if (!bandConfig) {
                continue;
            }
            for (auto& [networkName, tosLevel] : bandConfig->NetworkToTosLevel) {
                auto it = std::find(NetworkNames_.begin(), NetworkNames_.end(), networkName);
                if (it == NetworkNames_.end()) {
                    NetworkNames_.push_back(networkName);
                }
            }
        }

        for (auto band : TEnumTraits<EMultiplexingBand>::GetDomainValues()) {
            const auto& bandConfig = config->MultiplexingBands[band];
            auto& bandDescriptor = BandToDescriptor_[band];
            bandDescriptor.DefaultTosLevel = bandConfig ? bandConfig->TosLevel : DefaultTosLevel;
            bandDescriptor.NetworkIdToTosLevel.resize(NetworkNames_.size(), bandDescriptor.DefaultTosLevel);

            if (!bandConfig) {
                continue;
            }

            for (auto& [networkName, tosLevel] : bandConfig->NetworkToTosLevel) {
                auto it = std::find(NetworkNames_.begin(), NetworkNames_.end(), networkName);
                YCHECK(it != NetworkNames_.end());
                auto id = std::distance(NetworkNames_.begin(), it);
                bandDescriptor.NetworkIdToTosLevel[id] = tosLevel;
            }
        }
    }

    TTosLevel GetTosLevelForBand(EMultiplexingBand band, TNetworkId networkId)
    {
        TReaderGuard guard(SpinLock_);
        const auto& bandDescriptor = BandToDescriptor_[band];
        return bandDescriptor.NetworkIdToTosLevel[networkId];
    }

    TNetworkId GetNetworkId(const TString& networkName)
    {
        {
            TReaderGuard guard(SpinLock_);
            auto it = std::find(NetworkNames_.begin(), NetworkNames_.end(), networkName);
            if (it != NetworkNames_.end()) {
                return std::distance(NetworkNames_.begin(), it);
            }
        }

        TWriterGuard guard(SpinLock_);
        return DoRegisterNetwork(networkName);
    }

    const IInvokerPtr& GetLightInvoker()
    {
        return LightQueue_->GetInvoker();
    }

    const IInvokerPtr& GetHeavyInvoker()
    {
        return HeavyPool_->GetInvoker();
    }

    void Shutdown()
    {
        LightQueue_->Shutdown();
        HeavyPool_->Shutdown();
    }

private:
    struct TBandDescriptor
    {
        SmallVector<TTosLevel, 8> NetworkIdToTosLevel;
        TTosLevel DefaultTosLevel = DefaultTosLevel;
    };

    const TActionQueuePtr LightQueue_ = New<TActionQueue>("RpcLight");
    const TThreadPoolPtr HeavyPool_ = New<TThreadPool>(TDispatcherConfig::DefaultHeavyPoolSize, "RpcHeavy");

    TReaderWriterSpinLock SpinLock_;
    TEnumIndexedVector<TBandDescriptor, EMultiplexingBand> BandToDescriptor_;

    // Using linear search in vector since number of networks is very small.
    SmallVector<TString, 8> NetworkNames_;

    TNetworkId DoRegisterNetwork(const TString& networkName)
    {
        auto it = std::find(NetworkNames_.begin(), NetworkNames_.end(), networkName);
        if (it != NetworkNames_.end()) {
            return std::distance(NetworkNames_.begin(), it);
        }

        TNetworkId id = NetworkNames_.size();
        NetworkNames_.push_back(networkName);
        for (auto band : TEnumTraits<EMultiplexingBand>::GetDomainValues()) {
            auto& bandDescriptor = BandToDescriptor_[band];
            bandDescriptor.NetworkIdToTosLevel.resize(NetworkNames_.size(), bandDescriptor.DefaultTosLevel);
        }
        return id;
    }
};

TDispatcher::TDispatcher()
    : Impl_(new TImpl())
{ }

TDispatcher::~TDispatcher() = default;

TDispatcher* TDispatcher::Get()
{
    return Singleton<TDispatcher>();
}

void TDispatcher::StaticShutdown()
{
    Get()->Shutdown();
}

void TDispatcher::Configure(const TDispatcherConfigPtr& config)
{
    Impl_->Configure(config);
}

TTosLevel TDispatcher::GetTosLevelForBand(EMultiplexingBand band, TNetworkId networkId)
{
    return Impl_->GetTosLevelForBand(band, networkId);
}

TNetworkId TDispatcher::GetNetworkId(const TString& networkName)
{
    return Impl_->GetNetworkId(networkName);
}

void TDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

const IInvokerPtr& TDispatcher::GetLightInvoker()
{
    return Impl_->GetLightInvoker();
}

const IInvokerPtr& TDispatcher::GetHeavyInvoker()
{
    return Impl_->GetHeavyInvoker();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(7, TDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
