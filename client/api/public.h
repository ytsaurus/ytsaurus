#pragma once

#include <yt/core/misc/public.h>

namespace NYP::NClient::NApi {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TPodSpec_TResourceRequests;
class TPodSpec_TDiskVolumeRequest;
class TPodSpec_TGpuRequest;
class TPodSpec_TIP6AddressRequest;
class TPodSpec_TIP6SubnetRequest;

class TPodStatus_TGpuAllocation;

class TPodSpec_THostDevice;
class TPodSpec_TSysctlProperty;

class TTvmConfig;

} // namespace NProto

using TObjectId = TString;
using TTransactionId = NYT::TGuid;

constexpr int MaxObjectIdLength = 256;
constexpr int MaxNodeShortNameLength = 250;
constexpr int MaxPodFqdnLength = 630;

DEFINE_ENUM(EErrorCode,
    ((InvalidObjectId)             (100000))
    ((DuplicateObjectId)           (100001))
    ((NoSuchObject)                (100002))
    ((NotEnoughResources)          (100003))
    ((InvalidObjectType)           (100004))
    ((AuthenticationError)         (100005))
    ((AuthorizationError)          (100006))
    ((InvalidTransactionState)     (100007))
    ((InvalidTransactionId)        (100008))
    ((InvalidObjectState)          (100009))
    ((NoSuchTransaction)           (100010))
    ((UserBanned)                  (100011))
    ((AccountLimitExceeded)        (100012))
    ((PodSchedulingFailure)        (100013))
    ((PrerequisiteCheckFailure)    (100014))
    ((InvalidContinuationToken)    (100015))
    ((RowsAlreadyTrimmed)          (100016))
    ((InvalidObjectSpec)           (100017))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi

namespace NInfra::NPodAgent::API {

class TEnvVar;
class TPodAgentSpec;

} // namespace NInfra::NPodAgent::API
