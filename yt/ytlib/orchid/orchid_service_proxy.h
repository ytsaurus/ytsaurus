#pragma once

#include "common.h"
#include <ytlib/orchid/orchid_service.pb.h>

#include <core/rpc/client.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "OrchidService";
    }

    explicit TOrchidServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
