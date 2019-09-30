#pragma once

#include "public.h"

#include <yt/core/ytree/yson_serializable.h>

#include <yt/core/net/config.h>

#include <yt/core/rpc/config.h>

#include <yt/core/logging/config.h>

#include <yt/core/tracing/config.h>

#include <yt/core/profiling/config.h>

#include <yt/ytlib/chunk_client/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSingletonsConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    THashMap<TString, int> FiberStackPoolSizes;
    NNet::TAddressResolverConfigPtr AddressResolver;
    NRpc::TDispatcherConfigPtr RpcDispatcher;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NProfiling::TProfileManagerConfigPtr ProfileManager;
    NLogging::TLogManagerConfigPtr Logging;
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
        RegisterParameter("profile_manager", ProfileManager)
            .DefaultNew();
        RegisterParameter("logging", Logging)
            .Default(NLogging::TLogManagerConfig::CreateDefault());
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
