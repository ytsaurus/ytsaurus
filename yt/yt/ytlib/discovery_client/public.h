#pragma once

#include <yt/core/misc/public.h>

#include <yt/core/ypath/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TMemberInfo;
class TGroupMeta;
class TListMembersOptions;

class TReqListMembers;
class TRspListMembers;

class TReqGetGroupMeta;
class TRspGetGroupMeta;

class TReqHeartbeat;
class TRspHeartbeat;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TMemberClientConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryClientConfig)
DECLARE_REFCOUNTED_CLASS(TMemberClient)
DECLARE_REFCOUNTED_CLASS(TDiscoveryClient)

////////////////////////////////////////////////////////////////////////////////

using TGroupId = NYPath::TYPath;
using TMemberId = TString;

////////////////////////////////////////////////////////////////////////////////

static const TString PriorityAttribute = "priority";
static const TString RevisionAttribute = "revision";
static const TString LastHeartbeatTimeAttribute = "last_heartbeat_time";
static const TString LastAttributesUpdateTimeAttribute = "last_attributes_update_time";

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
