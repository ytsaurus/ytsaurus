#include "cluster_resources.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/master/chunk_server/chunk_manager.h>
#include <yt/server/master/chunk_server/medium.h>

#include <yt/server/lib/security_server/proto/security_manager.pb.h>

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
    : DiskSpace_{}
    , NodeCount(0)
    , ChunkCount(0)
    , TabletCount(0)
    , TabletStaticMemory(0)
    , MasterMemory(0)
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

TClusterResources&& TClusterResources::SetMasterMemory(i64 masterMemory) &&
{
    MasterMemory = masterMemory;
    return std::move(*this);
}

TClusterResources&& TClusterResources::SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&
{
    SetMediumDiskSpace(mediumIndex, diskSpace);
    return std::move(*this);
}

void TClusterResources::SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &
{
    if (diskSpace == 0) {
        DiskSpace_.erase(mediumIndex);
    } else {
        DiskSpace_[mediumIndex] = diskSpace;
    }
}

void TClusterResources::AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta)
{
    auto it = DiskSpace_.find(mediumIndex);
    if (it == DiskSpace_.end()) {
        if (diskSpaceDelta != 0) {
            DiskSpace_.insert({mediumIndex, diskSpaceDelta});
        }
    } else {
        it->second += diskSpaceDelta;
        if (it->second == 0) {
            DiskSpace_.erase(it);
        }
    }
}

void TClusterResources::ClearDiskSpace()
{
    DiskSpace_.clear();
}

const NChunkClient::TMediumMap<i64>& TClusterResources::DiskSpace() const
{
    return DiskSpace_;
}

/*static*/ TClusterResources TClusterResources::Infinite()
{
    auto resources = TClusterResources()
        .SetNodeCount(std::numeric_limits<i64>::max() / 2)
        .SetTabletCount(std::numeric_limits<int>::max() / 2)
        .SetChunkCount(std::numeric_limits<i64>::max() / 2)
        .SetTabletStaticMemory(std::numeric_limits<i64>::max() / 2)
        .SetMasterMemory(std::numeric_limits<i64>::max() / 2);
    for (int mediumIndex = 0; mediumIndex < NChunkClient::MaxMediumCount; ++mediumIndex) {
        resources.SetMediumDiskSpace(mediumIndex, std::numeric_limits<i64>::max() / 2);
    }
    return resources;
}

void TClusterResources::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DiskSpace_);
    Save(context, NodeCount);
    Save(context, ChunkCount);
    Save(context, TabletCount);
    Save(context, TabletStaticMemory);
    Save(context, MasterMemory);
}

void TClusterResources::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    // COMPAT(aozeritsky)
    if (context.GetVersion() < NCellMaster::EMasterReign::FixDenseMapSerialization) {
        auto mediumCount = Load<int>(context);
        for (auto i = 0; i < mediumCount; ++i) {
            auto space = Load<i64>(context);
            auto mediumIndex = Load<int>(context);
            if (space != 0) {
                DiskSpace_[mediumIndex] = space;
            }
        }
    } else {
        Load(context, DiskSpace_);
    }

    Load(context, NodeCount);
    Load(context, ChunkCount);
    Load(context, TabletCount);
    Load(context, TabletStaticMemory);
    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= NCellMaster::EMasterReign::MasterMemoryUsageAccounting) {
        Load(context, MasterMemory);
    }
}

void TClusterResources::Save(NCypressServer::TBeginCopyContext& context) const
{
    using NYT::Save;
    Save(context, static_cast<int>(DiskSpace_.size()));
    for (auto [mediumIndex, space] : DiskSpace_) {
        Save(context, space);
        Save(context, mediumIndex);
    }
    Save(context, NodeCount);
    Save(context, ChunkCount);
    Save(context, TabletCount);
    Save(context, TabletStaticMemory);
    Save(context, MasterMemory);
}

void TClusterResources::Load(NCypressServer::TEndCopyContext& context)
{
    using NYT::Load;
    auto mediumCount = Load<int>(context);
    for (auto i = 0; i < mediumCount; ++i) {
        auto space = Load<i64>(context);
        auto mediumIndex = Load<int>(context);
        DiskSpace_[mediumIndex] = space;
    }
    Load(context, NodeCount);
    Load(context, ChunkCount);
    Load(context, TabletCount);
    Load(context, TabletStaticMemory);
    Load(context, MasterMemory);
}

bool TClusterResources::IsAtLeastOneResourceLessThan(const TClusterResources& rhs) const
{
    if (this == &rhs) {
        return false;
    }

    for (const auto& [mediumIndex, lhsDiskSpace] : DiskSpace()) {
        auto rhsDiskSpace = rhs.DiskSpace().lookup(mediumIndex);
        if (lhsDiskSpace < rhsDiskSpace) {
            return true;
        }
    }

    for (const auto& [mediumIndex, rhsDiskSpace] : rhs.DiskSpace()) {
        auto lhsDiskSpace = DiskSpace().lookup(mediumIndex);
        if (lhsDiskSpace < rhsDiskSpace) {
            return true;
        }
    }

    return
        NodeCount < rhs.NodeCount ||
        ChunkCount < rhs.ChunkCount ||
        TabletCount < rhs.TabletCount ||
        TabletStaticMemory < rhs.TabletStaticMemory ||
        MasterMemory < rhs.MasterMemory;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources)
{
    protoResources->set_chunk_count(resources.ChunkCount);
    protoResources->set_node_count(resources.NodeCount);
    protoResources->set_tablet_count(resources.TabletCount);
    protoResources->set_tablet_static_memory_size(resources.TabletStaticMemory);
    protoResources->set_master_memory(resources.MasterMemory);

    for (const auto& [index, diskSpace] : resources.DiskSpace()) {
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
    resources->MasterMemory = protoResources.master_memory();

    resources->ClearDiskSpace();
    for (const auto& spaceStats : protoResources.disk_space_per_medium()) {
        resources->SetMediumDiskSpace(spaceStats.medium_index(), spaceStats.disk_space());
    }
}

////////////////////////////////////////////////////////////////////////////////

TSerializableClusterResources::TSerializableClusterResources(bool serializeDiskSpace)
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
    if (serializeDiskSpace) {
        RegisterParameter("disk_space", DiskSpace_)
            .Optional();
    }
    RegisterParameter("master_memory", MasterMemory_)
        .GreaterThanOrEqual(0)
        .Optional();

    RegisterPostprocessor([&] {
        for (const auto& pair : DiskSpacePerMedium_) {
            ValidateDiskSpace(pair.second);
        }
    });
}

TSerializableClusterResources::TSerializableClusterResources(
    const NChunkServer::TChunkManagerPtr& chunkManager,
    const TClusterResources& clusterResources,
    bool serializeDiskSpace)
    : TSerializableClusterResources(serializeDiskSpace)
{
    NodeCount_ = clusterResources.NodeCount;
    ChunkCount_ = clusterResources.ChunkCount;
    TabletCount_ = clusterResources.TabletCount;
    TabletStaticMemory_ = clusterResources.TabletStaticMemory;
    MasterMemory_ = clusterResources.MasterMemory;
    DiskSpace_ = 0;
    for (const auto& [mediumIndex, mediumDiskSpace] : clusterResources.DiskSpace()) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || medium->GetCache()) {
            continue;
        }
        YT_VERIFY(DiskSpacePerMedium_.insert(std::make_pair(medium->GetName(), mediumDiskSpace)).second);
        if (serializeDiskSpace) {
            DiskSpace_ += mediumDiskSpace;
        }
    }
}

TClusterResources TSerializableClusterResources::ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const
{
    auto result = TClusterResources()
        .SetNodeCount(NodeCount_)
        .SetChunkCount(ChunkCount_)
        .SetTabletCount(TabletCount_)
        .SetTabletStaticMemory(TabletStaticMemory_)
        .SetMasterMemory(MasterMemory_);
    for (const auto& [mediumName, mediumDiskSpace] : DiskSpacePerMedium_) {
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        result.SetMediumDiskSpace(medium->GetIndex(), mediumDiskSpace);
    }
    return result;
}

void TSerializableClusterResources::AddToMediumDiskSpace(const TString& mediumName, i64 mediumDiskSpace)
{
    DiskSpacePerMedium_[mediumName] += mediumDiskSpace;
}

////////////////////////////////////////////////////////////////////////////////

TClusterResources& operator += (TClusterResources& lhs, const TClusterResources& rhs)
{
    for (const auto& [mediumIndex, diskSpace] : rhs.DiskSpace()) {
        lhs.AddToMediumDiskSpace(mediumIndex, diskSpace);
    }
    lhs.NodeCount += rhs.NodeCount;
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.TabletCount += rhs.TabletCount;
    lhs.TabletStaticMemory += rhs.TabletStaticMemory;
    lhs.MasterMemory += rhs.MasterMemory;
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
    for (const auto& [mediumIndex, diskSpace] : rhs.DiskSpace()) {
        lhs.AddToMediumDiskSpace(mediumIndex, -diskSpace);
    }
    lhs.NodeCount -= rhs.NodeCount;
    lhs.ChunkCount -= rhs.ChunkCount;
    lhs.TabletCount -= rhs.TabletCount;
    lhs.TabletStaticMemory -= rhs.TabletStaticMemory;
    lhs.MasterMemory -= rhs.MasterMemory;
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
    for (const auto& [mediumIndex, diskSpace] : lhs.DiskSpace()) {
        lhs.SetMediumDiskSpace(mediumIndex, diskSpace * rhs);
    }
    lhs.NodeCount *= rhs;
    lhs.ChunkCount *= rhs;
    lhs.TabletCount *= rhs;
    lhs.TabletStaticMemory *= rhs;
    lhs.MasterMemory *= rhs;
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
    for (const auto& [mediumIndex, diskSpace] : resources.DiskSpace()) {
        result.SetMediumDiskSpace(mediumIndex, -diskSpace);
    }
    result.NodeCount = -resources.NodeCount;
    result.ChunkCount = -resources.ChunkCount;
    result.TabletCount = -resources.TabletCount;
    result.TabletStaticMemory = -resources.TabletStaticMemory;
    result.MasterMemory = -resources.MasterMemory;
    return result;
}

bool operator == (const TClusterResources& lhs, const TClusterResources& rhs)
{
    if (&lhs == &rhs) {
        return true;
    }
    if (lhs.DiskSpace().size() != rhs.DiskSpace().size()) {
        return false;
    }
    for (const auto& [mediumIndex, mediumDiskSpace] : lhs.DiskSpace()) {
        const auto it = rhs.DiskSpace().find(mediumIndex);
        if (it == rhs.DiskSpace().end() || it->second != mediumDiskSpace) {
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
    if (lhs.MasterMemory != rhs.MasterMemory) {
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
    for (const auto& [mediumIndex, diskSpace] : resources.DiskSpace()) {
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
    builder->AppendFormat("], NodeCount: %v, ChunkCount: %v, TabletCount: %v, TabletStaticMemory: %v, MasterMemory: %v}",
        resources.NodeCount,
        resources.ChunkCount,
        resources.TabletCount,
        resources.TabletStaticMemory,
        resources.MasterMemory);
}

TString ToString(const TClusterResources& resources)
{
    return ToStringViaBuilder(resources);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

