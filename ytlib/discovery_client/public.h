#pragma once

#include <yt/core/misc/public.h>

namespace NYT::NDiscoveryClient {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TMemberInfo;
class TListMembersOptions;

class TReqListGroups;
class TRspListGroups;

class TReqListMembers;
class TRspListMembers;

class TReqGetGroupSize;
class TRspGetGroupSize;

class TReqHeartbeat;
class TRspHeartbeat;

} // namespace NProto

DECLARE_REFCOUNTED_CLASS(TMemberClientConfig)
DECLARE_REFCOUNTED_CLASS(TDiscoveryClientConfig)
DECLARE_REFCOUNTED_CLASS(TMemberClient)
DECLARE_REFCOUNTED_CLASS(TDiscoveryClient)

////////////////////////////////////////////////////////////////////////////////

using TGroupId = TString;
using TMemberId = TString;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
