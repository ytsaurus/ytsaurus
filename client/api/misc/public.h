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

class TAntiaffinityConstraint;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

using TObjectId = TString;
using TTransactionId = NYT::TGuid;

constexpr int MaxObjectIdLength = 256;
constexpr int MaxNodeShortNameLength = 250;
constexpr int MaxPodFqdnLength = 630;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((InvalidObjectId)                 (100000))
    ((DuplicateObjectId)               (100001))
    ((NoSuchObject)                    (100002))
    ((NotEnoughResources)              (100003))
    ((InvalidObjectType)               (100004))
    ((AuthenticationError)             (100005))
    ((AuthorizationError)              (100006))
    ((InvalidTransactionState)         (100007))
    ((InvalidTransactionId)            (100008))
    ((InvalidObjectState)              (100009))
    ((NoSuchTransaction)               (100010))
    ((UserBanned)                      (100011))
    ((AccountLimitExceeded)            (100012))
    ((PodSchedulingFailure)            (100013))
    ((PrerequisiteCheckFailure)        (100014))
    ((InvalidContinuationToken)        (100015))
    ((RowsAlreadyTrimmed)              (100016))
    ((InvalidObjectSpec)               (100017))
    ((ContinuationTokenVersionMismatch)(100018))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EHfsmState,
    ((Unknown)           (  0))
    ((Initial)           (100))
    ((Up)                (200))
    ((Down)              (300))
    ((Suspected)         (400))
    ((PrepareMaintenance)(500))
    ((Maintenance)       (600))
    ((Probation)         (700))
);

// Must be kept in sync with protos
DEFINE_ENUM(EObjectType,
    ((Null)                   (-1))
    ((Node)                    (0))
    ((Pod)                     (1))
    ((PodSet)                  (2))
    ((Resource)                (3))
    ((NetworkProject)          (4))
    ((Endpoint)                (5))
    ((EndpointSet)             (6))
    ((NodeSegment)             (7))
    ((VirtualService)          (8))
    ((User)                    (9))
    ((Group)                  (10))
    ((InternetAddress)        (11))
    ((Account)                (12))
    ((ReplicaSet)             (13))
    ((DnsRecordSet)           (14))
    ((ResourceCache)          (15))
    ((MultiClusterReplicaSet) (16))
    ((DynamicResource)        (17))
    // Node2 is an alias and must be processed at the Api layer without explicit declaration.
    // ((Node2)                  (18))
    ((Stage)                  (19))
    ((PodDisruptionBudget)    (20))
    ((IP4AddressPool)         (21))
    ((NetworkModule)         (100)) // internal, not present in data_model.proto
    ((Schema)                (256))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NClient::NApi

namespace NInfra::NPodAgent::API {

class TEnvVar;
class TPodAgentSpec;

} // namespace NInfra::NPodAgent::API
