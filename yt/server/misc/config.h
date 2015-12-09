#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/config.h>

#include <yt/core/misc/address.h>

#include <yt/core/rpc/config.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAddressResolverConfigPtr AddressResolver;
    NRpc::TServerConfigPtr RpcServer;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;

    TServerConfig()
    {
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("rpc_server", RpcServer)
            .DefaultNew();
        RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
