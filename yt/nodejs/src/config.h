#pragma once

#include <core/ytree/yson_serializable.h>

#include <ytlib/driver/config.h>

#include <ytlib/chunk_client/config.h>

#include <core/misc/address.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class THttpProxyConfig
    : public NYTree::TYsonSerializable
{
public:
    NYTree::INodePtr Logging;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NDriver::TDriverConfigPtr Driver;
    TAddressResolverConfigPtr AddressResolver;

    THttpProxyConfig()
    {
        RegisterParameter("logging", Logging);
        RegisterParameter("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();
        RegisterParameter("driver", Driver)
            .DefaultNew();
        RegisterParameter("address_resolver", AddressResolver)
            .DefaultNew();
    }
};

typedef TIntrusivePtr<THttpProxyConfig> THttpProxyConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
