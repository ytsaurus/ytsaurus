#include "dispatcher.h"
#include "dispatcher_impl.h"

#include <yt/yt/core/bus/private.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

TTcpDispatcher::TTcpDispatcher()
    : Impl_(New<TImpl>())
{
    BusProfiler.WithSparse().AddProducer("", Impl_);
}

TTcpDispatcher* TTcpDispatcher::Get()
{
    return LeakySingleton<TTcpDispatcher>();
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

} // namespace NYT::NBus
