#pragma once

#include "common.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IServer;
typedef TIntrusivePtr<IServer> IServerPtr;

struct IService;
typedef TIntrusivePtr<IService> IServicePtr;

struct IServiceContext;
typedef TIntrusivePtr<IServiceContext> IServiceContextPtr;

struct IChannel;
typedef TIntrusivePtr<IChannel> IChannelPtr;

class TChannelCache;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
