#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

#define ITERATE_NODE_RESOURCES(XX) \
    XX(user_slots,            UserSlots) \
    XX(cpu,                   Cpu) \
    XX(gpu,                   Gpu) \
    XX(user_memory,           UserMemory) \
    XX(system_memory,         SystemMemory) \
    XX(network,               Network) \
    XX(replication_slots,     ReplicationSlots) \
    XX(replication_data_size, ReplicationDataSize) \
    XX(merge_data_size,       MergeDataSize) \
    XX(removal_slots,         RemovalSlots) \
    XX(repair_slots,          RepairSlots) \
    XX(repair_data_size,      RepairDataSize) \
    XX(seal_slots,            SealSlots) \
    XX(merge_slots,           MergeSlots) \
    XX(autotomy_slots,        AutotomySlots) \
    XX(vcpu,                  VCpu) \
    XX(reincarnation_slots,   ReincarnationSlots)

#define ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX) \
    XX(cpu,                   Cpu) \
    XX(gpu,                   Gpu) \
    XX(network,               Network) \
    XX(replication_slots,     ReplicationSlots) \
    XX(replication_data_size, ReplicationDataSize) \
    XX(merge_data_size,       MergeDataSize) \
    XX(removal_slots,         RemovalSlots) \
    XX(repair_slots,          RepairSlots) \
    XX(repair_data_size,      RepairDataSize) \
    XX(seal_slots,            SealSlots) \
    XX(user_memory,           UserMemory) \
    XX(system_memory,         SystemMemory) \
    XX(merge_slots,           MergeSlots) \
    XX(autotomy_slots,        AutotomySlots) \
    XX(reincarnation_slots,   ReincarnationSlots)

// NB(cherepashka): Removed system_memory from iterating construction described above due to the difficulties of integrating this attribute in the dynamic config.
#define ITERATE_NODE_RESOURCE_LIMITS_DYNAMIC_CONFIG_OVERRIDES(XX) \
    XX(cpu,                   Cpu) \
    XX(gpu,                   Gpu) \
    XX(network,               Network) \
    XX(replication_slots,     ReplicationSlots) \
    XX(replication_data_size, ReplicationDataSize) \
    XX(merge_data_size,       MergeDataSize) \
    XX(removal_slots,         RemovalSlots) \
    XX(repair_slots,          RepairSlots) \
    XX(repair_data_size,      RepairDataSize) \
    XX(seal_slots,            SealSlots) \
    XX(user_memory,           UserMemory) \
    XX(merge_slots,           MergeSlots) \
    XX(autotomy_slots,        AutotomySlots) \
    XX(reincarnation_slots,   ReincarnationSlots)

// NB: Types must be numbered from 0 to N - 1.
DEFINE_ENUM(EResourceType,
    (UserSlots)
    (Cpu)
    (Memory)
    (Network)
    (ReplicationSlots)
    (ReplicationDataSize)
    (RemovalSlots)
    (RepairSlots)
    (RepairDataSize)
    (SealSlots)
    (Gpu)
    (MergeSlots)
    (AutotomySlots)
    (ReincarnationSlots)
);

TString FormatResourceUsage(
    const NProto::TNodeResources& usage,
    const NProto::TNodeResources& limits);
TString FormatResourceUsage(
    const NProto::TNodeResources& usage,
    const NProto::TNodeResources& limits,
    const NProto::TDiskResources& diskResources);
TString FormatResources(const NProto::TNodeResources& resources);
TString ToString(
    const NProto::TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);

void ProfileResources(NProfiling::ISensorWriter* writer, const NProto::TNodeResources& resources);

const NProto::TNodeResources& ZeroNodeResources();
const NProto::TNodeResources& InfiniteNodeResources();

NObjectClient::TObjectId ObjectIdFromNodeId(TNodeId nodeId, NObjectClient::TCellTag);
TNodeId NodeIdFromObjectId(NObjectClient::TObjectId objectId);

void ValidateNodeTags(const std::vector<TString>& tags);

const TString& FormatShortly(ENodeFlavor flavor);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

TNodeResources  operator +  (const TNodeResources& lhs, const TNodeResources& rhs);
TNodeResources& operator += (TNodeResources& lhs, const TNodeResources& rhs);

TNodeResources  operator -  (const TNodeResources& lhs, const TNodeResources& rhs);
TNodeResources& operator -= (TNodeResources& lhs, const TNodeResources& rhs);

TNodeResources  operator *  (const TNodeResources& lhs, i64 rhs);
TNodeResources  operator *  (const TNodeResources& lhs, double rhs);
TNodeResources& operator *= (TNodeResources& lhs, i64 rhs);
TNodeResources& operator *= (TNodeResources& lhs, double rhs);

TNodeResources  operator -  (const TNodeResources& resources);

bool operator == (const TNodeResources& a, const TNodeResources& b);
bool operator != (const TNodeResources& a, const TNodeResources& b);

NProto::TNodeResources MakeNonnegative(const NProto::TNodeResources& resources);
bool Dominates(const NProto::TNodeResources& lhs, const TNodeResources& rhs);

TNodeResources Max(const TNodeResources& a, const TNodeResources& b);
TNodeResources Min(const TNodeResources& a, const TNodeResources& b);

void Serialize(
    const NProto::TNodeResources& resources,
    NYson::IYsonConsumer* consumer);

void Serialize(
    const NProto::TNodeResourceLimitsOverrides& overrides,
    NYson::IYsonConsumer* consumer);

void Deserialize(
    NProto::TNodeResourceLimitsOverrides& overrides,
    NYTree::INodePtr node);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
