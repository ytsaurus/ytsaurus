#include "dispatcher.h"
#include "dispatcher_impl.h"

#include <yt/yt/core/bus/private.h>

#include <yt/yt/core/misc/singleton.h>
#include <yt/yt/core/misc/shutdown.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TTcpDispatcher()
    : Impl_(New<TImpl>())
{
    BusProfiler.WithSparse().AddProducer("", Impl_);
}

TTcpDispatcher::~TTcpDispatcher() = default;

TTcpDispatcher* TTcpDispatcher::Get()
{
    return Singleton<TTcpDispatcher>();
}

void TTcpDispatcher::StaticShutdown()
{
    Get()->Shutdown();
}

void TTcpDispatcher::Shutdown()
{
    Impl_->Shutdown();
}

void TTcpDispatcher::Configure(const TTcpDispatcherConfigPtr& config)
{
    Impl_->Configure(config);
}

const TTcpDispatcherCountersPtr& TTcpDispatcher::GetCounters(const TString& networkName)
{
    return Impl_->GetCounters(networkName);
}

NConcurrency::IPollerPtr TTcpDispatcher::GetXferPoller()
{
    return Impl_->GetXferPoller();
}

void TTcpDispatcher::DisableNetworking()
{
    Impl_->DisableNetworking();
}

bool TTcpDispatcher::IsNetworkingDisabled()
{
    return Impl_->IsNetworkingDisabled();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(6, TTcpDispatcher::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
