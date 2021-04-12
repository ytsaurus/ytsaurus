#include "cluster_resources.h"
#include "cluster_resource_limits.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NTabletServer;

using NChunkClient::MaxMediumCount;
using NChunkServer::DefaultStoreMediumIndex;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TClusterResources::TClusterResources()
    : NodeCount_(0)
    , ChunkCount_(0)
    , TabletCount_(0)
    , TabletStaticMemory_(0)
    , DetailedMasterMemory_{}
    , DiskSpace_{}
{ }

TClusterResources&& TClusterResources::SetDetailedMasterMemory(const TDetailedMasterMemory& detailedMasterMemory) &&
{
    DetailedMasterMemory_ = detailedMasterMemory;
    return std::move(*this);
}

TClusterResources&& TClusterResources::SetDetailedMasterMemory(EMasterMemoryType type, i64 masterMemory) &&
{
    DetailedMasterMemory_[type] = masterMemory;
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

i64 TClusterResources::GetTotalMasterMemory() const
{
    return DetailedMasterMemory_.GetTotal();
}

void TClusterResources::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DiskSpace_);
    Save(context, NodeCount_);
    Save(context, ChunkCount_);
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
    Save(context, DetailedMasterMemory_);
}

void TClusterResources::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, DiskSpace_);
    Load(context, NodeCount_);
    Load(context, ChunkCount_);
    Load(context, TabletCount_);
    Load(context, TabletStaticMemory_);
    // COMPAT(aleksandra-zh)
    if (context.GetVersion() < NCellMaster::EMasterReign::DetailedMasterMemory) {
        Load<i64>(context);
    } else {
        Load(context, DetailedMasterMemory_);
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
    Save(context, NodeCount_);
    Save(context, ChunkCount_);
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
    Save(context, DetailedMasterMemory_);
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
    Load(context, NodeCount_);
    Load(context, ChunkCount_);
    Load(context, TabletCount_);
    Load(context, TabletStaticMemory_);
    Load(context, DetailedMasterMemory_);
}

TClusterResources& TClusterResources::operator += (const TClusterResources& other)
{
    for (const auto& [mediumIndex, diskSpace] : other.DiskSpace()) {
        AddToMediumDiskSpace(mediumIndex, diskSpace);
    }
    NodeCount_ += other.NodeCount_;
    ChunkCount_ += other.ChunkCount_;
    TabletCount_ += other.TabletCount_;
    TabletStaticMemory_ += other.TabletStaticMemory_;
    DetailedMasterMemory_ += other.DetailedMasterMemory_;
    return *this;
}

TClusterResources TClusterResources::operator + (const TClusterResources& other) const
{
    auto result = *this;
    result += other;
    return result;
}

TClusterResources& TClusterResources::operator -= (const TClusterResources& other)
{
    for (const auto& [mediumIndex, diskSpace] : other.DiskSpace()) {
        AddToMediumDiskSpace(mediumIndex, -diskSpace);
    }
    NodeCount_ -= other.NodeCount_;
    ChunkCount_ -= other.ChunkCount_;
    TabletCount_ -= other.TabletCount_;
    TabletStaticMemory_ -= other.TabletStaticMemory_;
    DetailedMasterMemory_ -= other.DetailedMasterMemory_;
    return *this;
}

TClusterResources TClusterResources::operator - (const TClusterResources& other) const
{
    auto result = *this;
    result -= other;
    return result;
}

TClusterResources& TClusterResources::operator *= (i64 other)
{
    for (const auto& [mediumIndex, diskSpace] : DiskSpace()) {
        SetMediumDiskSpace(mediumIndex, diskSpace * other);
    }
    NodeCount_ *= other;
    ChunkCount_ *= other;
    TabletCount_ *= other;
    TabletStaticMemory_ *= other;
    DetailedMasterMemory_ *= other;
    return *this;
}

TClusterResources TClusterResources::operator * (i64 other) const
{
    auto result = *this;
    result *= other;
    return result;
}

TClusterResources TClusterResources::operator - () const
{
    TClusterResources result;
    for (const auto& [mediumIndex, diskSpace] : DiskSpace()) {
        result.SetMediumDiskSpace(mediumIndex, -diskSpace);
    }
    result.NodeCount_ = -NodeCount_;
    result.ChunkCount_ = -ChunkCount_;
    result.TabletCount_ = -TabletCount_;
    result.TabletStaticMemory_ = -TabletStaticMemory_;
    result.DetailedMasterMemory_ = -DetailedMasterMemory_;
    return result;
}

bool TClusterResources::operator == (const TClusterResources& other) const
{
    if (this == &other) {
        return true;
    }
    if (DiskSpace().size() != other.DiskSpace().size()) {
        return false;
    }
    for (const auto& [mediumIndex, mediumDiskSpace] : DiskSpace()) {
        const auto it = other.DiskSpace().find(mediumIndex);
        if (it == other.DiskSpace().end() || it->second != mediumDiskSpace) {
            return false;
        }
    }
    if (NodeCount_ != other.NodeCount_) {
        return false;
    }
    if (ChunkCount_ != other.ChunkCount_) {
        return false;
    }
    if (TabletCount_ != other.TabletCount_) {
        return false;
    }
    if (TabletStaticMemory_ != other.TabletStaticMemory_) {
        return false;
    }
    if (DetailedMasterMemory_ != other.DetailedMasterMemory_) {
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TClusterResources* protoResources, const TClusterResources& resources)
{
    protoResources->set_chunk_count(resources.GetChunkCount());
    protoResources->set_node_count(resources.GetNodeCount());
    protoResources->set_tablet_count(resources.GetTabletCount());
    protoResources->set_tablet_static_memory_size(resources.GetTabletStaticMemory());
    ToProto(protoResources->mutable_detailed_master_memory(), resources.DetailedMasterMemory());

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
    resources->SetChunkCount(protoResources.chunk_count());
    resources->SetNodeCount(protoResources.node_count());
    resources->SetTabletCount(protoResources.tablet_count());
    resources->SetTabletStaticMemory(protoResources.tablet_static_memory_size());
    resources->DetailedMasterMemory() = FromProto<TDetailedMasterMemory>(protoResources.detailed_master_memory());

    resources->ClearDiskSpace();
    for (const auto& spaceStats : protoResources.disk_space_per_medium()) {
        resources->SetMediumDiskSpace(spaceStats.medium_index(), spaceStats.disk_space());
    }
}

////////////////////////////////////////////////////////////////////////////////

TSerializableClusterResources::TSerializableClusterResources(bool serializeTabletResources)
{
    RegisterParameter("node_count", NodeCount_)
        .Default(0)
        .GreaterThanOrEqual(0);
    RegisterParameter("chunk_count", ChunkCount_)
        .Default(0)
        .GreaterThanOrEqual(0);

    // COMPAT(ifsmirnov)
    if (serializeTabletResources) {
        RegisterParameter("tablet_count", TabletCount_)
            .Default(0)
            .GreaterThanOrEqual(0);
        RegisterParameter("tablet_static_memory", TabletStaticMemory_)
            .Default(0)
            .GreaterThanOrEqual(0);
    }

    RegisterParameter("disk_space_per_medium", DiskSpacePerMedium_)
        .Optional();
    RegisterParameter("disk_space", DiskSpace_)
        .Optional();
    RegisterParameter("master_memory", MasterMemory_)
        .Optional();
    RegisterParameter("detailed_master_memory", DetailedMasterMemory_)
        .Optional();

    RegisterPostprocessor([&] {
        for (const auto& [medium, diskSpace] : DiskSpacePerMedium_) {
            ValidateDiskSpace(diskSpace);
        }
        if (DetailedMasterMemory_.IsNegative()) {
            THROW_ERROR_EXCEPTION("Detailed master memory %v is negative",
                DetailedMasterMemory_);
        }
    });
}

TSerializableClusterResources::TSerializableClusterResources(
    const NChunkServer::TChunkManagerPtr& chunkManager,
    const TClusterResources& clusterResources,
    bool serializeTabletResources)
    : TSerializableClusterResources(serializeTabletResources)
{
    NodeCount_ = clusterResources.GetNodeCount();
    ChunkCount_ = clusterResources.GetChunkCount();
    TabletCount_ = clusterResources.GetTabletCount();
    TabletStaticMemory_ = clusterResources.GetTabletStaticMemory();
    MasterMemory_ = clusterResources.DetailedMasterMemory().GetTotal();
    DetailedMasterMemory_ = clusterResources.DetailedMasterMemory();
    DiskSpace_ = 0;
    for (const auto& [mediumIndex, mediumDiskSpace] : clusterResources.DiskSpace()) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || medium->GetCache()) {
            continue;
        }
        YT_VERIFY(DiskSpacePerMedium_.emplace(medium->GetName(), mediumDiskSpace).second);
        DiskSpace_ += mediumDiskSpace;
    }
}

TClusterResources TSerializableClusterResources::ToClusterResources(const NChunkServer::TChunkManagerPtr& chunkManager) const
{
    auto result = TClusterResources()
        .SetNodeCount(NodeCount_)
        .SetChunkCount(ChunkCount_)
        .SetTabletCount(TabletCount_)
        .SetTabletStaticMemory(TabletStaticMemory_)
        .SetDetailedMasterMemory(DetailedMasterMemory_);
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

TRichClusterResources::TRichClusterResources(
    const TClusterResources& clusterResources,
    const TTabletResources& tabletResources)
    : ClusterResources(clusterResources)
    , TabletResources(tabletResources)
{
    YT_VERIFY(ClusterResources.GetTabletCount() == 0);
    YT_VERIFY(ClusterResources.GetTabletStaticMemory() == 0);
}

////////////////////////////////////////////////////////////////////////////////

TSerializableRichClusterResources::TSerializableRichClusterResources()
    : TSerializableClusterResources(/*serializeTabletResources*/ false)
    , TSerializableTabletResources()
{ }

TSerializableRichClusterResources::TSerializableRichClusterResources(
    const NChunkServer::TChunkManagerPtr& chunkManager,
    const TRichClusterResources& richClusterResources)
    : TSerializableClusterResources(
        chunkManager,
        richClusterResources.ClusterResources,
        /*serializeTabletResources*/ false)
    , TSerializableTabletResources(richClusterResources.TabletResources)
{ }

TRichClusterResources TSerializableRichClusterResources::ToRichClusterResources(
    const NChunkServer::TChunkManagerPtr& chunkManager) const
{
    auto clusterResources = ToClusterResources(chunkManager);
    auto tabletResources = static_cast<TTabletResources>(*this);
    return {clusterResources, tabletResources};
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TClusterResources& resources, TStringBuf /*format*/)
{
    builder->AppendString(TStringBuf("{DiskSpace: ["));
    auto firstDiskSpace = true;
    for (const auto& [mediumIndex, diskSpace] : resources.DiskSpace()) {
        if (diskSpace != 0) {
            if (!firstDiskSpace) {
                builder->AppendString(TStringBuf(", "));
            }
            builder->AppendFormat("%v@%v",
                diskSpace,
                mediumIndex);
            firstDiskSpace = false;
        }
    }
    builder->AppendFormat("], NodeCount: %v, ChunkCount: %v, TabletCount: %v, TabletStaticMemory: %v, DetailedMasterMemory: %v}",
        resources.GetNodeCount(),
        resources.GetChunkCount(),
        resources.GetTabletCount(),
        resources.GetTabletStaticMemory(),
        resources.DetailedMasterMemory());
}

TString ToString(const TClusterResources& resources)
{
    return ToStringViaBuilder(resources);
}

////////////////////////////////////////////////////////////////////////////////

TRichClusterResources& operator += (TRichClusterResources& lhs, const TRichClusterResources& rhs)
{
    lhs.ClusterResources += rhs.ClusterResources;
    lhs.TabletResources += rhs.TabletResources;
    return lhs;
}

TRichClusterResources operator + (const TRichClusterResources& lhs, const TRichClusterResources& rhs)
{
    auto result = lhs;
    return result += rhs;
}

////////////////////////////////////////////////////////////////////////////////

TTabletResources ConvertToTabletResources(const TClusterResources& clusterResources)
{
    YT_VERIFY(clusterResources.GetNodeCount() == 0);
    YT_VERIFY(clusterResources.GetChunkCount() == 0);
    for (const auto& [key, value] : clusterResources.DiskSpace()) {
        YT_VERIFY(value == 0);
    }

    YT_VERIFY(clusterResources.DetailedMasterMemory().IsZero());

    return TTabletResources()
        .SetTabletCount(clusterResources.GetTabletCount())
        .SetTabletStaticMemory(clusterResources.GetTabletStaticMemory());
}

TClusterResources ConvertToClusterResources(const TTabletResources& tabletResources)
{
    return TClusterResources()
        .SetTabletCount(tabletResources.TabletCount)
        .SetTabletStaticMemory(tabletResources.TabletStaticMemory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
