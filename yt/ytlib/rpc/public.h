#pragma once

#include "common.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class IServer;
typedef TIntrusivePtr<IServer> IServerPtr;

class IService;
typedef TIntrusivePtr<IService> IServicePtr;

struct IServiceContext;
typedef TIntrusivePtr<IServiceContext> IServiceContextPtr;

class IChannel;
typedef TIntrusivePtr<IChannel> IChannelPtr;

class TChannelCache;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
