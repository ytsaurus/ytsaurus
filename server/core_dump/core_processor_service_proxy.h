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
    DEFINE_RPC_PROXY(TCoreProcessorServiceProxy, CoreProcessorService);

    DEFINE_RPC_PROXY_METHOD(NCoreDump::NProto, StartCoreDump);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCoreDump
} // namespace NYT
