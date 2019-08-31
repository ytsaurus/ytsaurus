#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

#include <yp/client/api/public.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

using TPodResourceRequests = NClient::NApi::NProto::TPodSpec_TResourceRequests;

using TPodDiskVolumeRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest>;

using TPodGpuRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TGpuRequest>;

using TPodIP6AddressRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TIP6AddressRequest>;

using TPodIP6SubnetRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TIP6SubnetRequest>;

////////////////////////////////////////////////////////////////////////////////

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

DEFINE_ENUM(ENodeMaintenanceState,
    ((None)              (  0))
    ((Requested)         (100))
    ((Acknowledged)      (200))
    ((InProgress)        (300))
);

DEFINE_STRING_SERIALIZABLE_ENUM(EResourceKind,
    ((Undefined)      (-1))
    ((Cpu)             (0))
    ((Memory)          (1))
    ((Disk)            (2))
    ((Slot)            (3))
    ((Gpu)             (4))
    ((Network)         (5))
);

////////////////////////////////////////////////////////////////////////////////

using NClient::NApi::TObjectId;

struct TObjectFilter;

constexpr int TypicalDiskResourceCountPerNode = 16;
constexpr int TypicalGpuResourceCountPerNode = 16;

using NYT::NTransactionClient::TTimestamp;
using NYT::NTransactionClient::NullTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
