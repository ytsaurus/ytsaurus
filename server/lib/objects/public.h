#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

#include <yp/client/api/misc/public.h>

#include <yt/ytlib/transaction_client/public.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

using TPodResourceRequests = NClient::NApi::NProto::TPodSpec_TResourceRequests;

using TNodeAlerts = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TNodeAlert>;

using TPodDiskVolumeRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TDiskVolumeRequest>;

using TPodGpuRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TGpuRequest>;

using TPodGpuAllocations = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodStatus_TGpuAllocation>;

using TPodIP6AddressRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TIP6AddressRequest>;

using TPodIP6SubnetRequests = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TIP6SubnetRequest>;

using TSchedulingHints = ::google::protobuf::RepeatedPtrField<
    NClient::NApi::NProto::TPodSpec_TSchedulingHint>;

////////////////////////////////////////////////////////////////////////////////

using NClient::NApi::EEvictionReason;
using NClient::NApi::EHfsmState;
using NClient::NApi::EObjectType;

DEFINE_ENUM(ENodeMaintenanceState,
    ((None)              (  0))
    ((Requested)         (100))
    ((Acknowledged)      (200))
    ((InProgress)        (300))
);

DEFINE_ENUM(EPodMaintenanceState,
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

TObjectId GenerateUuid();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects

////////////////////////////////////////////////////////////////////////////////

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <>
TString FormatEnum<NYP::NServer::NObjects::EObjectType>(
    NYP::NServer::NObjects::EObjectType value,
    typename TEnumTraits<NYP::NServer::NObjects::EObjectType>::TType*);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
