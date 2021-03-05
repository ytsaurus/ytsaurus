#include "dispatcher.h"
#include "config.h"

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/spinlock.h>
#include <yt/yt/core/concurrency/fair_share_thread_pool.h>

#include <yt/yt/core/misc/lazy_ptr.h>
#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/shutdown.h>
#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NRpc {

using namespace NConcurrency;
using namespace NBus;
using namespace NServiceDiscovery;

////////////////////////////////////////////////////////////////////////////////

class TDispatcher::TImpl
{
public:
    TImpl()
        : CompressionPoolInvoker_(BIND([this] {
            return CreatePrioritizedInvoker(CompressionPool_->GetInvoker());
        }))
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
        CompressionPool_->Configure(config->CompressionPoolSize);
        FairShareCompressionPool_->Configure(config->CompressionPoolSize);

        {
            auto guard = WriterGuard(SpinLock_);

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

                // Possible overwrite values for default network, filled in ctor.
                for (int networkId = 0; networkId < NetworkNames_.size(); ++networkId) {
                    bandDescriptor.NetworkIdToTosLevel[networkId] = bandDescriptor.DefaultTosLevel;
                }

                if (!bandConfig) {
                    continue;
                }

                for (auto& [networkName, tosLevel] : bandConfig->NetworkToTosLevel) {
                    auto it = std::find(NetworkNames_.begin(), NetworkNames_.end(), networkName);
                    YT_VERIFY(it != NetworkNames_.end());
                    auto id = std::distance(NetworkNames_.begin(), it);
                    bandDescriptor.NetworkIdToTosLevel[id] = tosLevel;
                }
            }
        }
    }

    TTosLevel GetTosLevelForBand(EMultiplexingBand band, TNetworkId networkId)
    {
        auto guard = ReaderGuard(SpinLock_);
        const auto& bandDescriptor = BandToDescriptor_[band];
        return bandDescriptor.NetworkIdToTosLevel[networkId];
    }

    TNetworkId GetNetworkId(const TString& networkName)
    {
        {
            auto guard = ReaderGuard(SpinLock_);
            auto it = std::find(NetworkNames_.begin(), NetworkNames_.end(), networkName);
            if (it != NetworkNames_.end()) {
                return std::distance(NetworkNames_.begin(), it);
            }
        }

        auto guard = WriterGuard(SpinLock_);
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

    const IPrioritizedInvokerPtr& GetPrioritizedCompressionPoolInvoker()
    {
        return CompressionPoolInvoker_.Value();
    }

    const IFairShareThreadPoolPtr& GetFairShareCompressionThreadPool()
    {
        return FairShareCompressionPool_;
    }

    const IInvokerPtr& GetCompressionPoolInvoker()
    {
        return CompressionPool_->GetInvoker();
    }

    IServiceDiscoveryPtr GetServiceDiscovery()
    {
        return ServiceDiscovery_.Load();
    }

    void SetServiceDiscovery(IServiceDiscoveryPtr serviceDiscovery)
    {
        ServiceDiscovery_.Store(std::move(serviceDiscovery));
    }

    void Shutdown()
    {
        LightQueue_->Shutdown();
        HeavyPool_->Shutdown();
        CompressionPool_->Shutdown();
        FairShareCompressionPool_->Shutdown();
    }

private:
    struct TBandDescriptor
    {
        SmallVector<TTosLevel, 8> NetworkIdToTosLevel;
        TTosLevel DefaultTosLevel = DefaultTosLevel;
    };

    const TActionQueuePtr LightQueue_ = New<TActionQueue>("RpcLight");
    const TThreadPoolPtr HeavyPool_ = New<TThreadPool>(TDispatcherConfig::DefaultHeavyPoolSize, "RpcHeavy");
    const TThreadPoolPtr CompressionPool_ = New<TThreadPool>(TDispatcherConfig::DefaultCompressionPoolSize, "Compression");
    const IFairShareThreadPoolPtr FairShareCompressionPool_ = CreateFairShareThreadPool(TDispatcherConfig::DefaultCompressionPoolSize, "FSCompression");

    TLazyIntrusivePtr<IPrioritizedInvoker> CompressionPoolInvoker_;

    YT_DECLARE_SPINLOCK(TReaderWriterSpinLock, SpinLock_);
    TEnumIndexedVector<EMultiplexingBand, TBandDescriptor> BandToDescriptor_;

    // Using linear search in vector since number of networks is very small.
    SmallVector<TString, 8> NetworkNames_;

    TAtomicObject<IServiceDiscoveryPtr> ServiceDiscovery_;

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

const IPrioritizedInvokerPtr& TDispatcher::GetPrioritizedCompressionPoolInvoker()
{
    return Impl_->GetPrioritizedCompressionPoolInvoker();
}

const IInvokerPtr& TDispatcher::GetCompressionPoolInvoker()
{
    return Impl_->GetCompressionPoolInvoker();
}

const IFairShareThreadPoolPtr& TDispatcher::GetFairShareCompressionThreadPool()
{
    return Impl_->GetFairShareCompressionThreadPool();
}

IServiceDiscoveryPtr TDispatcher::GetServiceDiscovery()
{
    return Impl_->GetServiceDiscovery();
}

void TDispatcher::SetServiceDiscovery(IServiceDiscoveryPtr serviceDiscovery)
{
    Impl_->SetServiceDiscovery(std::move(serviceDiscovery));
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(7, TDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
