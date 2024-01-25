#pragma once

#include "public.h"

#include <yt/yt/ytlib/incumbent_client/public.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NIncumbentServer {

////////////////////////////////////////////////////////////////////////////////

class TIncumbentSchedulingConfig
    : public NYTree::TYsonStruct
{
public:
    //! If true, incumbent manager will try to schedule this incumbent
    //! to one of the followers (if any).
    //! If false, incumbent will be always scheduled to leader.
    bool UseFollowers;

    //! Logical weight of the incumbent. Incumbent manager will try to
    //! make total weight at followers as equal as possible.
    int Weight;

    REGISTER_YSON_STRUCT(TIncumbentSchedulingConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIncumbentSchedulingConfig)

////////////////////////////////////////////////////////////////////////////////

class TIncumbentSchedulerConfig
    : public NYTree::TYsonStruct
{
public:
    TEnumIndexedArray<NIncumbentClient::EIncumbentType, TIncumbentSchedulingConfigPtr> Incumbents;

    //! If less than |MinAliveFollowers| followers are active according to leader,
    //! no incumbents are scheduled to followers.
    int MinAliveFollowers;

    REGISTER_YSON_STRUCT(TIncumbentSchedulerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIncumbentSchedulerConfig)

////////////////////////////////////////////////////////////////////////////////

class TIncumbentManagerConfig
    : public NYTree::TYsonStruct
{
public:
    TIncumbentSchedulerConfigPtr Scheduler;

    //! Period of incumbents assignment both to leader and followers.
    TDuration AssignPeriod;

    //! If peer does not receive a heartbeat within this duration,
    //! it drops all incumbents.
    TDuration PeerLeaseDuration;
    //! If leader is unable to report a heartbeat to peer within this
    //! period, it is considered offline.
    TDuration PeerGracePeriod;

    //! Peers listed here act like offline peers.
    THashSet<TString> BannedPeers;

    //! Timeout for heartbeat RPC request.
    TDuration HeartbeatTimeout;

    REGISTER_YSON_STRUCT(TIncumbentManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TIncumbentManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
