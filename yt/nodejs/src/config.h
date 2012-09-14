#pragma once

#include <ytlib/ytree/yson_serializable.h>
#include <ytlib/driver/config.h>
#include <ytlib/chunk_client/config.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

struct THttpProxyConfig
    : public TYsonSerializable
{
    NYTree::INodePtr Logging;
    NChunkClient::TDispatcherConfigPtr ChunkClientDispatcher;
    NDriver::TDriverConfigPtr Driver;

    THttpProxyConfig()
    {
        Register("logging", Logging);
        Register("chunk_client_dispatcher", ChunkClientDispatcher)
            .DefaultNew();
        Register("driver", Driver)
            .DefaultNew();
    }
};

typedef TIntrusivePtr<THttpProxyConfig> THttpProxyConfigPtr;

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
