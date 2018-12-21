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
        std::fill(BandToTosLevel_.begin(), BandToTosLevel_.end(), DefaultTosLevel);
    }

    void Configure(const TDispatcherConfigPtr& config)
    {
        HeavyPool_->Configure(config->HeavyPoolSize);
        for (auto band : TEnumTraits<EMultiplexingBand>::GetDomainValues()) {
            const auto& bandConfig = config->MultiplexingBands[band];
            BandToTosLevel_[band].store(bandConfig ? bandConfig->TosLevel : DefaultTosLevel);
        }
    }

    TTosLevel GetTosLevelForBand(EMultiplexingBand band)
    {
        return BandToTosLevel_[band].load();
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
    const TActionQueuePtr LightQueue_ = New<TActionQueue>("RpcLight");
    const TThreadPoolPtr HeavyPool_ = New<TThreadPool>(TDispatcherConfig::DefaultHeavyPoolSize, "RpcHeavy");

    TEnumIndexedVector<std::atomic<int>, EMultiplexingBand> BandToTosLevel_;
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

TTosLevel TDispatcher::GetTosLevelForBand(EMultiplexingBand band)
{
    return Impl_->GetTosLevelForBand(band);
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
