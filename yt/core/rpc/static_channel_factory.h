#pragma once

#include "public.h"
#include "channel.h"
#include "helpers.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TStaticChannelFactory
    : public IChannelFactory
{
public:
    TStaticChannelFactoryPtr Add(const TString& address, IChannelPtr channel);

    virtual IChannelPtr CreateChannel(const TString& address) override;
    virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override;

private:
    THashMap<TString, IChannelPtr> ChannelMap;

};

DEFINE_REFCOUNTED_TYPE(TStaticChannelFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
