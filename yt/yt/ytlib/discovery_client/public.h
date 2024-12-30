#pragma once

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TMemberInfo;
class TGroupMeta;
class TListMembersOptions;

class TReqListMembers;
class TRspListMembers;

class TReqListGroups;
class TRspListGroups;

class TReqGetGroupMeta;
class TRspGetGroupMeta;

class TReqHeartbeat;
class TRspHeartbeat;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TDiscoveryConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TMemberClientConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryClientConfig)
DECLARE_REFCOUNTED_CLASS(TServerAddressPool)

DECLARE_REFCOUNTED_STRUCT(IMemberClient)
DECLARE_REFCOUNTED_STRUCT(IDiscoveryClient)

////////////////////////////////////////////////////////////////////////////////

using TGroupId = NYPath::TYPath;
using TMemberId = std::string;

////////////////////////////////////////////////////////////////////////////////

inline const std::string PriorityAttribute = "priority";
inline const std::string RevisionAttribute = "revision";
inline const std::string LastHeartbeatTimeAttribute = "last_heartbeat_time";
inline const std::string LastAttributesUpdateTimeAttribute = "last_attributes_update_time";

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((NoSuchGroup)         (2300))
    ((NoSuchMember)        (2301))
    ((InvalidGroupId)      (2302))
    ((InvalidMemberId)     (2303))
    ((NodeLimitExceeded)   (2304))
    ((GroupLimitExceeded)  (2305))
    ((DepthLimitExceeded)  (2306))
    ((MemberLimitExceeded) (2307))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
