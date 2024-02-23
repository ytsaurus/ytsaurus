#include "helpers.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/client/node_tracker_client/helpers.h>
#include <yt/yt/client/node_tracker_client/private.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/arithmetic_formula.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/yson_struct.h>

#include <limits>

namespace NYT::NNodeTrackerClient {

using namespace NYson;
using namespace NYTree;
using namespace NProfiling;
using namespace NObjectClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

TString FormatMemoryUsage(i64 memoryUsage)
{
    TStringBuf prefix = "";
    if (memoryUsage < 0) {
        prefix = "-";
        memoryUsage *= -1;
    }
    if (memoryUsage < static_cast<i64>(1_KB)) {
        return Format("%v%vB", prefix, memoryUsage);
    }
    if (memoryUsage < static_cast<i64>(1_MB)) {
        return Format("%v%vKB", prefix, memoryUsage / 1_KB);
    }
    return Format("%v%vMB", prefix, memoryUsage / 1_MB);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TString FormatResources(
    const TNodeResources& usage,
    const TNodeResources& limits)
{
    return Format(
        "UserSlots: %v/%v, Cpu: %v/%v, Gpu: %v/%v, UserMemory: %v/%v, SystemMemory: %v/%v, Network: %v/%v, "
        "ReplicationSlots: %v/%v, ReplicationDataSize: %v/%v, "
        "RemovalSlots: %v/%v, "
        "RepairSlots: %v/%v, RepairDataSize: %v/%v, "
        "SealSlots: %v/%v, "
        "MergeSlots: %v/%v, MergeDataSize: %v/%v, "
        "AutotomySlots: %v/%v, ReincarnationSlots: %v/%v",
        // User slots
        usage.user_slots(),
        limits.user_slots(),
        // Cpu
        usage.cpu(),
        limits.cpu(),
        // Gpu,
        usage.gpu(),
        limits.gpu(),
        // User memory
        FormatMemoryUsage(usage.user_memory()),
        FormatMemoryUsage(limits.user_memory()),
        // System memory
        FormatMemoryUsage(usage.system_memory()),
        FormatMemoryUsage(limits.system_memory()),
        // Network
        usage.network(),
        limits.network(),
        // Replication slots
        usage.replication_slots(),
        limits.replication_slots(),
        // Replication data size
        usage.replication_data_size(),
        limits.replication_data_size(),
        // Removal slots
        usage.removal_slots(),
        limits.removal_slots(),
        // Repair slots
        usage.repair_slots(),
        limits.repair_slots(),
        // Repair data size
        usage.repair_data_size(),
        limits.repair_data_size(),
        // Seal slots
        usage.seal_slots(),
        limits.seal_slots(),
        // Merge slots
        usage.merge_slots(),
        limits.merge_slots(),
        // Merge data size
        usage.merge_data_size(),
        limits.merge_data_size(),
        // Autotomy slots
        usage.autotomy_slots(),
        limits.autotomy_slots(),
        // Reincarnation slots
        usage.reincarnation_slots(),
        limits.reincarnation_slots());
}

TString FormatResourceUsage(
    const TNodeResources& usage,
    const TNodeResources& limits)
{
    return Format("{%v}", FormatResources(usage, limits));
}

TString ToString(const NProto::TDiskResources& diskResources, const NChunkClient::TMediumDirectoryPtr& mediumDirectory)
{
    return Format(
        "%v",
        MakeFormattableView(diskResources.disk_location_resources(), [&mediumDirectory] (TStringBuilderBase* builder, const NProto::TDiskLocationResources& locationResources) {
            int mediumIndex = locationResources.medium_index();
            auto* mediumDescriptor = mediumDirectory->FindByIndex(mediumIndex);
            TStringBuf mediumName = mediumDescriptor
                ? mediumDescriptor->Name
                : TStringBuf("unknown");
            builder->AppendFormat("{usage: %v, limit: %v, medium_index: %v, medium_name: %v}",
                locationResources.usage(),
                locationResources.limit(),
                mediumIndex,
                mediumName);
        })
    );
}

TString FormatResourceUsage(
    const TNodeResources& usage,
    const TNodeResources& limits,
    const TDiskResources& diskResources)
{
    return Format("{%v, DiskResources: %v}", FormatResources(usage, limits), diskResources);
}

TString FormatResources(const TNodeResources& resources)
{
    return Format(
        "{"
        "UserSlots: %v, Cpu: %v, Gpu: %v, UserMemory: %v, SystemMemory: %v, Network: %v, "
        "ReplicationSlots: %v, ReplicationDataSize: %v, "
        "RemovalSlots: %v, "
        "RepairSlots: %v, RepairDataSize: %v, "
        "SealSlots: %v, "
        "MergeSlots: %v, MergeDataSize: %v, "
        "AutotomySlots: %v, "
        "ReincarnationSlots: %v"
        "}",
        resources.user_slots(),
        resources.cpu(),
        resources.gpu(),
        FormatMemoryUsage(resources.user_memory()),
        FormatMemoryUsage(resources.system_memory()),
        resources.network(),
        resources.replication_slots(),
        FormatMemoryUsage(resources.replication_data_size()),
        resources.removal_slots(),
        resources.repair_slots(),
        FormatMemoryUsage(resources.repair_data_size()),
        resources.seal_slots(),
        resources.merge_slots(),
        FormatMemoryUsage(resources.merge_data_size()),
        resources.autotomy_slots(),
        resources.reincarnation_slots());
}

void ProfileResources(ISensorWriter* writer, const NProto::TNodeResources& resources)
{
    #define XX(name, Name) writer->AddGauge("/" #name, resources.name());
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
}

TNodeResources GetZeroNodeResources()
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(0);
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

const TNodeResources& ZeroNodeResources()
{
    static auto value = GetZeroNodeResources();
    return value;
}

TNodeResources GetInfiniteResources()
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(std::numeric_limits<decltype(result.name())>::max() / 4);
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

const TNodeResources& InfiniteNodeResources()
{
    static auto result = GetInfiniteResources();
    return result;
}

TObjectId ObjectIdFromNodeId(TNodeId nodeId, TCellTag cellTag)
{
    return MakeId(EObjectType::ClusterNode, cellTag, nodeId.Underlying(), 0);
}

TNodeId NodeIdFromObjectId(TObjectId objectId)
{
    return TNodeId(CounterFromId(objectId));
}

void ValidateNodeTags(const std::vector<TString>& tags)
{
    for (const auto& tag : tags) {
        try {
            ValidateBooleanFormulaVariable(tag);
        } catch (const std::exception& ex) {
            THROW_ERROR_EXCEPTION("Invalid node tag %Qv", tag)
                << ex;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

TNodeResources operator + (const TNodeResources& lhs, const TNodeResources& rhs)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(lhs.name() + rhs.name());
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

TNodeResources& operator += (TNodeResources& lhs, const TNodeResources& rhs)
{
    #define XX(name, Name) lhs.set_##name(lhs.name() + rhs.name());
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return lhs;
}

TNodeResources operator - (const TNodeResources& lhs, const TNodeResources& rhs)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(lhs.name() - rhs.name());
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

TNodeResources& operator -= (TNodeResources& lhs, const TNodeResources& rhs)
{
    #define XX(name, Name) lhs.set_##name(lhs.name() - rhs.name());
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return lhs;
}

TNodeResources operator * (const TNodeResources& lhs, i64 rhs)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(lhs.name() * rhs);
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

TNodeResources operator * (const TNodeResources& lhs, double rhs)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(static_cast<decltype(lhs.name())>(lhs.name() * rhs + 0.5));
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

TNodeResources& operator *= (TNodeResources& lhs, i64 rhs)
{
    #define XX(name, Name) lhs.set_##name(lhs.name() * rhs);
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return lhs;
}

TNodeResources& operator *= (TNodeResources& lhs, double rhs)
{
    #define XX(name, Name) lhs.set_##name(static_cast<decltype(lhs.name())>(lhs.name() * rhs + 0.5));
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return lhs;
}

TNodeResources  operator - (const TNodeResources& resources)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(-resources.name());
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

bool operator == (const TNodeResources& lhs, const TNodeResources& rhs)
{
    return
        #define XX(name, Name) lhs.name() == rhs.name() &&
        ITERATE_NODE_RESOURCES(XX)
        #undef XX
        true;
}

TNodeResources MakeNonnegative(const TNodeResources& resources)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(std::max(resources.name(), static_cast<decltype(resources.name())>(0)));
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

bool Dominates(const TNodeResources& lhs, const TNodeResources& rhs)
{
    return
        #define XX(name, Name) lhs.name() >= rhs.name() &&
        ITERATE_NODE_RESOURCES(XX)
        #undef XX
        true;
}

TNodeResources Max(const TNodeResources& a, const TNodeResources& b)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(std::max(a.name(), b.name()));
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

TNodeResources Min(const TNodeResources& a, const TNodeResources& b)
{
    TNodeResources result;
    #define XX(name, Name) result.set_##name(std::min(a.name(), b.name()));
    ITERATE_NODE_RESOURCES(XX)
    #undef XX
    return result;
}

void Serialize(const TNodeResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            #define XX(name, Name) .Item(#name).Value(resources.name())
            ITERATE_NODE_RESOURCES(XX)
            #undef XX
        .EndMap();
}

class TSerializableNodeResourceLimitsOverrides
    : public NYTree::TYsonStructLite
{
public:
    #define XX(name, Name) std::optional<decltype(TNodeResourceLimitsOverrides::default_instance().name())> Name;
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX

    REGISTER_YSON_STRUCT_LITE(TSerializableNodeResourceLimitsOverrides);

    static void Register(TRegistrar registrar)
    {
        #define XX(name, Name) \
            registrar.Parameter(#name, &TThis::Name) \
                .GreaterThanOrEqual(0) \
                .Optional();
        ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
        #undef XX
    }
};

void Serialize(const TNodeResourceLimitsOverrides& overrides, IYsonConsumer* consumer)
{
    TSerializableNodeResourceLimitsOverrides serializableOverrides;
    #define XX(name, Name) \
        if (overrides.has_##name()) { \
            serializableOverrides.Name = overrides.name(); \
        }
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX
    Serialize(serializableOverrides, consumer);
}

void Deserialize(TNodeResourceLimitsOverrides& overrides, INodePtr node)
{
    TSerializableNodeResourceLimitsOverrides serializableOverrides;
    Deserialize(serializableOverrides, node);
    #define XX(name, Name) \
        if (serializableOverrides.Name) { \
            overrides.set_##name(*serializableOverrides.Name); \
        } else { \
            overrides.clear_##name(); \
        }
    ITERATE_NODE_RESOURCE_LIMITS_OVERRIDES(XX)
    #undef XX
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
