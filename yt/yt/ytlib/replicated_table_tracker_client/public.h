#pragma once

#include <yt/yt/core/misc/error_code.h>

namespace NYT::NReplicatedTableTrackerClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReplicatedTableData;
class TReplicaData;
class TTableCollocationData;
class TReplicatedTableTrackerSnapshot;
class TChangeReplicaModeCommand;

class TTrackerStateUpdateAction;

class TReqGetTrackerStateUpdates;
class TRspGetTrackerStateUpdates;

class TReqApplyChangeReplicaModeCommands;
class TRspApplyChangeReplicaModeCommands;

class TReqComputeReplicaLagTimes;
class TRspComputeReplicaLagTimes;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((RttServiceDisabled)        (31000))
    ((StateRevisionMismatch)     (31001))
    ((NonResidentReplica)        (31002))
);

using TTrackerStateRevision = i64;
constexpr TTrackerStateRevision InvalidTrackerStateRevision = -1;
constexpr TTrackerStateRevision NullTrackerStateRevision = 0;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NReplicatedTableTrackerClient
