#pragma once

#include "public.h"

#include "structs.h"

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NZookeeper {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECommandType,
    ((None)                     (-1))
    ((Ping)                     (11))
    ((GetChildren2)             (12))
);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EZookeeperError,
    ((OK)                        (0))
    ((ApiError)               (-100))
);

////////////////////////////////////////////////////////////////////////////////

struct TReqStartSession
    : public TRefCounted
{
    int ProtocolVersion = -1;
    i64 LastZxidSeen = -1;
    TDuration Timeout = TDuration::Zero();
    i64 SessionId = -1;
    TString Password;
    bool ReadOnly = false;

    void Deserialize(IZookeeperProtocolReader* reader);
};

DEFINE_REFCOUNTED_TYPE(TReqStartSession)

struct TRspStartSession
    : public TRefCounted
{
    int ProtocolVersion = -1;
    TDuration Timeout = TDuration::Zero();
    i64 SessionId = -1;
    TString Password;
    bool ReadOnly = false;

    void Serialize(IZookeeperProtocolWriter* writer) const;
};

DEFINE_REFCOUNTED_TYPE(TRspStartSession)

////////////////////////////////////////////////////////////////////////////////

struct TReqPing
    : public TRefCounted
{
    void Deserialize(IZookeeperProtocolReader* reader);
};

DEFINE_REFCOUNTED_TYPE(TReqPing)

struct TRspPing
    : public TRefCounted
{
    i64 Zxid = -1;

    void Serialize(IZookeeperProtocolWriter* writer) const;
};

DEFINE_REFCOUNTED_TYPE(TRspPing)

////////////////////////////////////////////////////////////////////////////////

struct TReqGetChildren2
    : public TRefCounted
{
    TString Path;
    bool Watch = false;

    void Deserialize(IZookeeperProtocolReader* reader);
};

DEFINE_REFCOUNTED_TYPE(TReqGetChildren2)

struct TRspGetChildren2
    : public TRefCounted
{
    std::vector<TString> Children;
    TNodeStat Stat;

    i64 Zxid = -1;

    void Serialize(IZookeeperProtocolWriter* writer);
};

DEFINE_REFCOUNTED_TYPE(TRspGetChildren2)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeper
