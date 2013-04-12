#pragma once

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/driver/config.h>
#include <ytlib/chunk_client/config.h>
#include <ytlib/misc/address.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

class THttpProxyConfig
    : public TYsonSerializable
{
public:
    NYTree::INodePtr Logging;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NDriver::TDriverConfigPtr Driver;
    TAddressResolverConfigPtr AddressResolver;

    THttpProxyConfig()
    {
        Register("logging", Logging);
        Register("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();
        Register("driver", Driver)
            .DefaultNew();
        Register("address_resolver", AddressResolver)
            .DefaultNew();
    }
};

typedef TIntrusivePtr<THttpProxyConfig> THttpProxyConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
