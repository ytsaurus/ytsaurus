#pragma once

#include "public.h"

#include <core/misc/enum.h>

#include <core/logging/log.h>

#include <core/profiling/profiler.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

extern NLog::TLogger BusLogger;
extern NProfiling::TProfiler BusProfiler;

typedef TGuid TConnectionId;
typedef TGuid TPacketId;

class TTcpConnection;
typedef TIntrusivePtr<TTcpConnection> TTcpConnectionPtr;

struct IEventLoopObject;
typedef TIntrusivePtr<IEventLoopObject> IEventLoopObjectPtr;

class TTcpDispatcherThread;
typedef TIntrusivePtr<TTcpDispatcherThread> TTcpDispatcherThreadPtr;

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EConnectionType,
    (Client)
    (Server)
);

DECLARE_ENUM(EConnectionEvent,
    (AddressResolved)
    (Terminated)
    (MessageEnqueued)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

