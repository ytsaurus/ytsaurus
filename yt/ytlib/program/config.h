#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/net/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/logging/config.h>

#include <yt/core/tracing/config.h>

#include <yt/ytlib/chunk_client/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSingletonsConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    yhash<TString, int> FiberStackPoolSizes;
    NNet::TAddressResolverConfigPtr AddressResolver;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NLogging::TLogConfigPtr Logging;
    NTracing::TTraceManagerConfigPtr Tracing;

    TSingletonsConfig()
    {
        RegisterParameter("fiber_stack_pool_sizes", FiberStackPoolSizes)
            .Default({});
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("rpc_dispatcher", RpcDispatcher)
            .DefaultNew();
        RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();
        RegisterParameter("logging", Logging)
            .Default(NLogging::TLogConfig::CreateDefault());
        RegisterParameter("tracing", Tracing)
            .DefaultNew();
    }
};

DEFINE_REFCOUNTED_TYPE(TSingletonsConfig)

////////////////////////////////////////////////////////////////////////////////

void WarnForUnrecognizedOptions(
    const NLogging::TLogger& logger,
    const NYTree::TYsonSerializablePtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
