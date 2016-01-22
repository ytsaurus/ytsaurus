#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

#include <core/misc/address.h>

#include <core/bus/config.h>

#include <core/rpc/config.h>

#include <ytlib/chunk_client/config.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public virtual NYTree::TYsonSerializable
{
public:
    TAddressResolverConfigPtr AddressResolver;
    NBus::TTcpBusServerConfigPtr BusServer;
    NRpc::TServerConfigPtr RpcServer;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;

    //! RPC interface port number.
    int RpcPort;

    //! HTTP monitoring interface port number.
    int MonitoringPort;

    TServerConfig()
    {
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
        RegisterParameter("bus_server", BusServer)
            .DefaultNew();
        RegisterParameter("rpc_server", RpcServer)
            .DefaultNew();
        RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();

        RegisterInitializer([&] () {
            BusServer->Port = RpcPort;
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
