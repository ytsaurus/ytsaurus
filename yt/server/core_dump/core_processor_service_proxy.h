#pragma once

#include <yt/server/core_dump/core_processor_service.pb.h>

#include <yt/core/rpc/client.h>

namespace NYT {
namespace NCoreDump {

////////////////////////////////////////////////////////////////////////////////

class TCoreProcessorServiceProxy
    : public NRpc::TProxyBase
{
public:
    static Stroka GetServiceName()
    {
        return "CoreProcessorService";
    }

    static int GetProtocolVersion()
    {
        return 0;
    }

    explicit TCoreProcessorServiceProxy(NRpc::IChannelPtr channel)
        : TProxyBase(channel, GetServiceName(), GetProtocolVersion())
    { }

    DEFINE_RPC_PROXY_METHOD(NCoreDump::NProto, StartCoreDump);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
