#pragma once

#include "public.h"

#include <ytlib/logging/log.h>
#include <ytlib/profiling/profiler.h>

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

