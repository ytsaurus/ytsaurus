#pragma once

#include <yt/yt/core/misc/public.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IClient)
DECLARE_REFCOUNTED_STRUCT(IConnection)
DECLARE_REFCOUNTED_STRUCT(IDriver)
DECLARE_REFCOUNTED_STRUCT(IServer)
DECLARE_REFCOUNTED_STRUCT(ISession)
DECLARE_REFCOUNTED_STRUCT(ISessionManager)

DECLARE_REFCOUNTED_CLASS(TZookeeperConfig)

struct IZookeeperProtocolReader;
struct IZookeeperProtocolWriter;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TReqStartSession)
DECLARE_REFCOUNTED_STRUCT(TRspStartSession)
DECLARE_REFCOUNTED_STRUCT(TReqPing)
DECLARE_REFCOUNTED_STRUCT(TRspPing)
DECLARE_REFCOUNTED_STRUCT(TReqGetChildren2)
DECLARE_REFCOUNTED_STRUCT(TRspGetChildren2)

////////////////////////////////////////////////////////////////////////////////

using TSessionId = i64;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
