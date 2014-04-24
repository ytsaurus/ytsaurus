#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/misc/address.h>

#include <core/rpc/config.h>

#include <ytlib/chunk_client/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public virtual TYsonSerializable
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
