#include "cluster_resources.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

using namespace NYson;

using NChunkClient::MaxMediumCount;
using NChunkServer::DefaultStoreMediumIndex;

////////////////////////////////////////////////////////////////////////////////

namespace {

void ValidateDiskSpace(i64 diskSpace)
{
    if (diskSpace < 0) {
        THROW_ERROR_EXCEPTION("Invalid disk space size: expected >= 0, found %v",
            diskSpace);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TClusterResources::TClusterResources()
    : DiskSpace{}
    , NodeCount(0)
    , ChunkCount(0)
    , TabletCount(0)
    , TabletStaticMemory(0)
{ }

TClusterResources&& TClusterResources::SetNodeCount(i64 nodeCount) &&
{
    NodeCount = nodeCount;
    return std::move(*this);
}

TClusterResources&& TClusterResources::SetChunkCount(i64 chunkCount) &&
{
    ChunkCount = chunkCount;
    return std::move(*this);
}

TClusterResources&& TClusterResources::SetTabletCount(int tabletCount) &&
{
    TabletCount = tabletCount;
    return std::move(*this);
}

TClusterResources&& TClusterResources::SetTabletStaticMemory(i64 tabletStaticMemory) &&
{
    TabletStaticMemory = tabletStaticMemory;
    return std::move(*this);
}

TClusterResources&& TClusterResources::SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&
{
    DiskSpace[mediumIndex] = diskSpace;
    return std::move(*this);
}

void TClusterResources::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, static_cast<int>(DiskSpace.size()));
    for (const auto& [mediumIndex, space] : DiskSpace) {
        Save(context, space);
        Save(context, mediumIndex);
    }
    Save(context, NodeCount);
    Save(context, ChunkCount);
    Save(context, TabletCount);
    Save(context, TabletStaticMemory);
}

void TClusterResources::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    // COMPAT(aozeritsky)
    if (context.GetVersion() < 832) {
        const auto oldMaxMediumCount = 7;
        std::array<i64, oldMaxMediumCount> oldDiskSpaceArray = {};
        Load(context, oldDiskSpaceArray);

        for (int mediumIndex = 0 ; mediumIndex < 7; ++mediumIndex) {
            if (oldDiskSpaceArray[mediumIndex] > 0) {
                DiskSpace[mediumIndex] = oldDiskSpaceArray[mediumIndex];
            }
        }
    } else {
        auto mediumCount = Load<int>(context);
        for (auto i = 0; i < mediumCount; ++i) {
            auto space = Load<i64>(context);
            auto mediumIndex = Load<int>(context);
            DiskSpace[mediumIndex] = space;
        }
    }
    // COMPAT(shakurov)
    if (context.GetVersion() < 818) {
        NodeCount = Load<int>(context);
        ChunkCount = Load<int>(context);
    } else {
        Load(context, NodeCount);
        Load(context, ChunkCount);
    }
    Load(context, TabletCount);
    Load(context, TabletStaticMemory);
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources)
{
    protoResources->set_chunk_count(resources.ChunkCount);
    protoResources->set_node_count(resources.NodeCount);
    protoResources->set_tablet_count(resources.TabletCount);
    protoResources->set_tablet_static_memory_size(resources.TabletStaticMemory);

    for (const auto& [index, diskSpace] : resources.DiskSpace) {
        if (diskSpace != 0) {
            auto* protoDiskSpace = protoResources->add_disk_space_per_medium();
            protoDiskSpace->set_medium_index(index);
            protoDiskSpace->set_disk_space(diskSpace);
        }
    }
}

void FromProto(TClusterResources* resources, const NProto::TClusterResources& protoResources)
{
    resources->ChunkCount = protoResources.chunk_count();
    resources->NodeCount = protoResources.node_count();
    resources->TabletCount = protoResources.tablet_count();
    resources->TabletStaticMemory = protoResources.tablet_static_memory_size();

    resources->DiskSpace.clear();
    for (const auto& spaceStats : protoResources.disk_space_per_medium()) {
        resources->DiskSpace[spaceStats.medium_index()] = spaceStats.disk_space();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSerializableClusterResources::TSerializableClusterResources()
{
    RegisterParameter("node_count", NodeCount_)
        .GreaterThanOrEqual(0);
    RegisterParameter("chunk_count", ChunkCount_)
        .GreaterThanOrEqual(0);
    RegisterParameter("tablet_count", TabletCount_)
        // COMPAT(savrus) add defaults to environment/init_cluster.py
        .Default(1000)
        .GreaterThanOrEqual(0);
    RegisterParameter("tablet_static_memory", TabletStaticMemory_)
        // COMPAT(savrus) add defaults to environment/init_cluster.py
        .Default(1_GB)
        .GreaterThanOrEqual(0);
    RegisterParameter("disk_space_per_medium", DiskSpacePerMedium_);
    // NB: this is for (partial) compatibility: 'disk_space' is serialized when
    // read, but ignored when set. Hence no validation.
    RegisterParameter("disk_space", DiskSpace_)
        .Optional();

    RegisterPostprocessor([&] {
        for (const auto& pair : DiskSpacePerMedium_) {
            ValidateDiskSpace(pair.second);
        }
    });
}

TSerializableClusterResources::TSerializableClusterResources(
    const NChunkServer::TChunkManagerPtr& chunkManager,
    const TClusterResources& clusterResources)
    : TSerializableClusterResources()
{
    NodeCount_ = clusterResources.NodeCount;
    ChunkCount_ = clusterResources.ChunkCount;
    TabletCount_ = clusterResources.TabletCount;
    TabletStaticMemory_ = clusterResources.TabletStaticMemory;
    DiskSpace_ = 0;
    for (const auto& [mediumIndex, mediumDiskSpace] : clusterResources.DiskSpace) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || medium->GetCache()) {
            continue;
        }
        YCHECK(DiskSpacePerMedium_.insert(std::make_pair(medium->GetName(), mediumDiskSpace)).second);
        DiskSpace_ += mediumDiskSpace;
    }
}

TClusterResources TSerializableClusterResources::ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const
{
    auto result = TClusterResources()
        .SetNodeCount(NodeCount_)
        .SetChunkCount(ChunkCount_)
        .SetTabletCount(TabletCount_)
        .SetTabletStaticMemory(TabletStaticMemory_);
    for (const auto& pair : DiskSpacePerMedium_) {
        auto* medium = chunkManager->GetMediumByNameOrThrow(pair.first);
        result.DiskSpace[medium->GetIndex()] = pair.second;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs)
{
    for (const auto& [mediumIndex, diskSpace] : rhs.DiskSpace) {
        lhs.DiskSpace[mediumIndex] += diskSpace;
    }
    lhs.NodeCount += rhs.NodeCount;
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.TabletCount += rhs.TabletCount;
    lhs.TabletStaticMemory += rhs.TabletStaticMemory;
    return lhs;
}

TClusterResources operator + (const TClusterResources& lhs, const TClusterResources& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TClusterResources& operator -= (TClusterResources& lhs, const TClusterResources& rhs)
{
    for (const auto& [mediumIndex, diskSpace] : rhs.DiskSpace) {
        lhs.DiskSpace[mediumIndex] -= diskSpace;
    }
    lhs.NodeCount -= rhs.NodeCount;
    lhs.ChunkCount -= rhs.ChunkCount;
    lhs.TabletCount -= rhs.TabletCount;
    lhs.TabletStaticMemory -= rhs.TabletStaticMemory;
    return lhs;
}

TClusterResources operator - (const TClusterResources& lhs, const TClusterResources& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TClusterResources& operator *= (TClusterResources& lhs, i64 rhs)
{
    for (auto& item : lhs.DiskSpace) {
        item.second *= rhs;
    }
    lhs.NodeCount *= rhs;
    lhs.ChunkCount *= rhs;
    lhs.TabletCount *= rhs;
    lhs.TabletStaticMemory *= rhs;
    return lhs;
}

TClusterResources operator * (const TClusterResources& lhs, i64 rhs)
{
    auto result = lhs;
    result *= rhs;
    return result;
}

TClusterResources operator -  (const TClusterResources& resources)
{
    TClusterResources result;
    for (const auto& [mediumIndex, diskSpace] : resources.DiskSpace) {
        result.DiskSpace[mediumIndex] = -diskSpace;
    }
    result.NodeCount = -resources.NodeCount;
    result.ChunkCount = -resources.ChunkCount;
    result.TabletCount = -resources.TabletCount;
    result.TabletStaticMemory = -resources.TabletStaticMemory;
    return result;
}

bool operator == (const TClusterResources& lhs, const TClusterResources& rhs)
{
    if (&lhs == &rhs) {
        return true;
    }
    if (lhs.DiskSpace.size() != rhs.DiskSpace.size()) {
        return false;
    }
    for (const auto& item : lhs.DiskSpace) {
        const auto it = rhs.DiskSpace.find(item.first);
        if (it == rhs.DiskSpace.end() || it->second != item.second) {
            return false;
        }
    }
    if (lhs.NodeCount != rhs.NodeCount) {
        return false;
    }
    if (lhs.ChunkCount != rhs.ChunkCount) {
        return false;
    }
    if (lhs.TabletCount != rhs.TabletCount) {
        return false;
    }
    if (lhs.TabletStaticMemory != rhs.TabletStaticMemory) {
        return false;
    }
    return true;
}

bool operator != (const TClusterResources& lhs, const TClusterResources& rhs)
{
    return !(lhs == rhs);
}

void FormatValue(TStringBuilderBase* builder, const TClusterResources& resources, TStringBuf /*format*/)
{
    builder->AppendString(AsStringBuf("{DiskSpace: ["));
    bool firstDiskSpace = true;
    for (const auto& [mediumIndex, diskSpace] : resources.DiskSpace) {
        if (diskSpace != 0) {
            if (!firstDiskSpace) {
                builder->AppendString(AsStringBuf(", "));
            }
            builder->AppendFormat("%v@%v",
                diskSpace,
                mediumIndex);
            firstDiskSpace = false;
        }
    }
    builder->AppendFormat("], NodeCount: %v, ChunkCount: %v, TabletCount: %v, TabletStaticMemory: %v}",
        resources.NodeCount,
        resources.ChunkCount,
        resources.TabletCount,
        resources.TabletStaticMemory);
}

TString ToString(const TClusterResources& resources)
{
    return ToStringViaBuilder(resources);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

