#pragma once

#include <core/rpc/caching_channel_factory.h>
#include <core/rpc/bus_channel.h>

#include <core/misc/singleton.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class TTag>
class TCachingBusChannelFactory
{
private:
    IChannelFactoryPtr Factory_;

    TCachingBusChannelFactory()
        : Factory_(CreateCachingChannelFactory(GetBusChannelFactory()))
    { }

    DECLARE_SINGLETON_MIXIN(TCachingBusChannelFactory, TStaticInstanceMixin);

public:
    static const IChannelFactoryPtr& Get()
    {
        return TSingleton::Get()->Factory_;
    }
};

template <class TTag>
IChannelFactoryPtr GetCachingBusChannelFactory()
{
    return TCachingBusChannelFactory<TTag>::Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT

