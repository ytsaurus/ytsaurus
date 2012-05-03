#pragma once

#include "common.h"
#include <ytlib/orchid/orchid_service.pb.h>

#include <ytlib/rpc/client.h>

namespace NYT {
namespace NOrchid {

////////////////////////////////////////////////////////////////////////////////

class TOrchidServiceProxy
    : public NRpc::TProxyBase
{
public:
    typedef TIntrusivePtr<TOrchidServiceProxy> TPtr;

    static Stroka GetServiceName()
    {
        return "OrchidService";
    }

    TOrchidServiceProxy(NRpc::IChannel* channel)
        : TProxyBase(channel, GetServiceName())
    { }

    DEFINE_RPC_PROXY_METHOD(NProto, Execute);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NOrchid
} // namespace NYT
