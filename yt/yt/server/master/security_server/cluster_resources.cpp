#include "cluster_resources.h"

#include "private.h"
#include "cluster_resource_limits.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/yson/public.h>

#include <util/generic/algorithm.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NYson;
using namespace NYTree;
using namespace NTabletServer;
using namespace NChunkServer;

using NChunkClient::MaxMediumCount;
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

#define XX(Name) void TClusterResources::Increase##Name(NMpl::TCallTraits<decltype(Name##_)>::TType delta) \
{ \
    Name##_ += delta; \
}
FOR_EACH_CLUSTER_RESOURCE(XX)
#undef XX

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

i64 TClusterResources::GetMediumDiskSpace(int mediumIndex) const
{
    auto it = DiskSpace_.find(mediumIndex);
    return it == DiskSpace_.end() ? 0 : it->second;
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

void TClusterResources::ClearMasterMemory()
{
    DetailedMasterMemory_ = {};
    ChunkHostCellMasterMemory_ = 0;
}

i64 TClusterResources::GetTotalMasterMemory() const
{
    return DetailedMasterMemory_.GetTotal();
}



TClusterResources::TMediaDiskSpace TClusterResources::GetPatchedDiskSpace(
    const IChunkManagerPtr& chunkManager,
    const TCompactVector<int, 4>& additionalMediumIndexes) const
{
    auto compareByMediumIndexes =
        [] (std::pair<const TMedium*, i64> a, std::pair<const TMedium*, i64> b) {
            return a.first->GetIndex() < b.first->GetIndex();
        };

    // Disk space patched with additional media.
    TCompactVector<std::pair<const TMedium*, i64>, 4> diskSpace;
    auto addMediumIndex = [&] (int mediumIndex, i64 mediumDiskSpace) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!IsObjectAlive(medium)) {
            return;
        }

        auto [beginIt, endIt] = std::equal_range(
            diskSpace.begin(),
            diskSpace.end(),
            std::pair<const TMedium*, i64>(medium, mediumDiskSpace),
            compareByMediumIndexes);

        if (auto distance = std::distance(beginIt, endIt); distance > 0) {
            YT_ASSERT(distance == 1); // No duplicate media.
            YT_ASSERT(mediumDiskSpace == 0);
            return;
        }

        diskSpace.insert(beginIt, {medium, mediumDiskSpace}); // No emplace in TCompactVector.
    };

    for (auto [mediumIndex, mediumDiskSpace] : DiskSpace()) {
        addMediumIndex(mediumIndex, mediumDiskSpace);
    }
    for (auto mediumIndex : additionalMediumIndexes) {
        addMediumIndex(mediumIndex, 0);
    }
    YT_ASSERT(std::is_sorted(diskSpace.begin(), diskSpace.end(), compareByMediumIndexes));
    return diskSpace;
}

void TClusterResources::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DiskSpace_);
    Save(context, NodeCount_);
    Save(context, ChunkCount_);
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
    Save(context, ChunkHostCellMasterMemory_);
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
    Load(context, ChunkHostCellMasterMemory_);
    Load(context, DetailedMasterMemory_);
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
    Save(context, ChunkHostCellMasterMemory_);
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
    Load(context, ChunkHostCellMasterMemory_);
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
    ChunkHostCellMasterMemory_ += other.ChunkHostCellMasterMemory_;
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
    ChunkHostCellMasterMemory_ -= other.ChunkHostCellMasterMemory_;
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
    ChunkHostCellMasterMemory_ *= other;
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
    result.ChunkHostCellMasterMemory_ = -ChunkHostCellMasterMemory_;
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
    if (ChunkHostCellMasterMemory_ != other.ChunkHostCellMasterMemory_) {
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
    protoResources->set_chunk_host_cell_master_memory(resources.GetChunkHostCellMasterMemory());
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
    resources->SetChunkHostCellMasterMemory(protoResources.chunk_host_cell_master_memory());
    resources->DetailedMasterMemory() = FromProto<TDetailedMasterMemory>(protoResources.detailed_master_memory());

    resources->ClearDiskSpace();
    for (const auto& spaceStats : protoResources.disk_space_per_medium()) {
        resources->SetMediumDiskSpace(spaceStats.medium_index(), spaceStats.disk_space());
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// When #multicellStatistics are provided, serializes master memory usage in a
// detailed way. #committed is only relevant iff #multicellStatistics is provided.
void DoSerializeClusterResources(
    const TClusterResources& clusterResources,
    const TAccountMulticellStatistics* multicellStatistics,
    bool committed,
    TFluentMap& fluent,
    const TBootstrap* bootstrap,
    bool serializeTabletResources,
    const TCompactVector<int, 4>& additionalMediumIndexes = {})
{
    const auto& chunkManager = bootstrap->GetChunkManager();
    const auto& multicellManager = bootstrap->GetMulticellManager();

    YT_ASSERT(!multicellStatistics ||
        clusterResources == std::accumulate(
            multicellStatistics->begin(),
            multicellStatistics->end(),
            TClusterResources{},
            [&] (const TClusterResources& total, const auto& pair) {
                return total + (committed ? pair.second.CommittedResourceUsage : pair.second.ResourceUsage);
            }));

    // Disk space patched with additional media.
    TCompactVector<std::pair<const TMedium*, i64>, 4> diskSpace = clusterResources.GetPatchedDiskSpace(
        chunkManager,
        additionalMediumIndexes);

    i64 totalDiskSpace = 0;
    for (const auto& [medium, size] : diskSpace) {
        totalDiskSpace += size;
    }

    fluent
        .Item("node_count").Value(clusterResources.GetNodeCount())
        .Item("chunk_count").Value(clusterResources.GetChunkCount())
        // COMPAT(ifsmirnov)
        .DoIf(serializeTabletResources, [&] (TFluentMap fluent) {
            fluent
                .Item("tablet_count").Value(clusterResources.GetTabletCount())
                .Item("tablet_static_memory").Value(clusterResources.GetTabletStaticMemory());
        })
        .Item("disk_space_per_medium").DoMapFor(
            diskSpace,
            [&] (TFluentMap fluent, auto pair) {
                auto [medium, mediumDiskSpace] = pair;
                fluent.Item(medium->GetName()).Value(mediumDiskSpace);
            })
        .Item("disk_space").Value(totalDiskSpace)
        .Item("chunk_host_cell_master_memory").Value(clusterResources.GetChunkHostCellMasterMemory())
        .DoIf(multicellStatistics, [&] (TFluentMap fluent) {
            i64 totalMasterMemory = clusterResources.DetailedMasterMemory().GetTotal();
            i64 chunkHostCellMasterMemory = clusterResources.GetChunkHostCellMasterMemory();
            fluent
                .Item("master_memory").BeginMap()
                    .Item("total").Value(totalMasterMemory)
                    .Item("chunk_host").Value(chunkHostCellMasterMemory)
                    .Item("per_cell").DoMapFor(
                        *multicellStatistics,
                        [&] (TFluentMap fluent, const auto& pair) {
                            const auto& [cellTag, accountStatistics] = pair;
                            const auto& cellMasterMemory = committed
                                ? accountStatistics.CommittedResourceUsage.GetTotalMasterMemory()
                                : accountStatistics.ResourceUsage.GetTotalMasterMemory();
                            fluent
                                .Item(multicellManager->GetMasterCellName(cellTag)).Value(cellMasterMemory);
                        })
                .EndMap();
        })
        .DoIf(!multicellStatistics, [&] (TFluentMap fluent) {
            fluent.Item("master_memory").Value(clusterResources.DetailedMasterMemory().GetTotal());
        })
        .Item("detailed_master_memory").Value(clusterResources.DetailedMasterMemory());
}

void DoDeserializeClusterResources(
    TClusterResources& clusterResources,
    INodePtr node,
    const TBootstrap* bootstrap,
    bool deserializeTabletResources)
{
    auto map = node->AsMap();

    auto result = TClusterResources()
        .SetNodeCount(GetOptionalNonNegativeI64ChildOrThrow(map, "node_count"))
        .SetChunkCount(GetOptionalNonNegativeI64ChildOrThrow(map, "chunk_count"));
    if (deserializeTabletResources) {
        result.SetTabletCount(GetOptionalNonNegativeI64ChildOrThrow(map, "tablet_count"));
        result.SetTabletStaticMemory(GetOptionalNonNegativeI64ChildOrThrow(map, "tablet_static_memory"));
    }

    if (auto child = map->FindChild("disk_space_per_medium")) {
        const auto& chunkManager = bootstrap->GetChunkManager();
        auto mediumToNode = child->AsMap()->GetChildren();
        for (const auto& [mediumName, diskSpaceNode] : mediumToNode) {
            const auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
            auto mediumDiskSpace = diskSpaceNode->AsInt64()->GetValue();
            ValidateDiskSpace(mediumDiskSpace);
            result.SetMediumDiskSpace(medium->GetIndex(), mediumDiskSpace);
        }
    }

    if (auto child = map->FindChild("chunk_host_cell_master_memory")) {
        result.SetChunkHostCellMasterMemory(child->AsInt64()->GetValue());
    }

    if (auto child = map->FindChild("detailed_master_memory")) {
        Deserialize(result.DetailedMasterMemory(), child);
        if (result.DetailedMasterMemory().IsNegative()) {
            THROW_ERROR_EXCEPTION("Detailed master memory cannot be negative, found %v",
                result.DetailedMasterMemory());
        }
    }

    clusterResources = result;
}

} // namespace

void SerializeClusterResources(
    const TClusterResources& resources,
    NYson::IYsonConsumer* consumer,
    const TBootstrap* bootstrap)
{
    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();

    DoSerializeClusterResources(
        resources,
        /*multicellStatistics*/ nullptr,
        /*committed*/ false, // Doesn't matter.
        fluent,
        bootstrap,
        /*serializeTabletResources*/ true);

    fluent
        .EndMap();
}

void SerializeRichClusterResources(
    const TRichClusterResources& resources,
    NYson::IYsonConsumer* consumer,
    const TBootstrap* bootstrap)
{
    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();

    DoSerializeClusterResources(
        resources.ClusterResources,
        /*multicellStatistics*/ nullptr,
        /*committed*/ false, // Doesn't matter.
        fluent,
        bootstrap,
        /*serializeTabletResources*/ false);

    Serialize(resources.TabletResources, fluent);

    fluent
        .EndMap();
}

void SerializeAccountClusterResourceUsage(
    const TAccount* account,
    bool committed,
    bool recursive,
    NYson::IYsonConsumer* consumer,
    const TBootstrap* bootstrap)
{
    const TClusterResources* clusterResources = nullptr;
    const TAccountMulticellStatistics* multicellStatistics = nullptr;

    TClusterResources nonRecursiveResourceUsage;
    TAccountMulticellStatistics nonRecursiveMulticellStatistics;

    const auto& clusterStatistics = account->ClusterStatistics();

    if (recursive) {
        clusterResources = committed
            ? &clusterStatistics.CommittedResourceUsage
            : &clusterStatistics.ResourceUsage;
        multicellStatistics = &account->MulticellStatistics();
    } else {
        nonRecursiveResourceUsage =
            (committed ? clusterStatistics.CommittedResourceUsage : clusterStatistics.ResourceUsage)
            - account->ComputeTotalChildrenResourceUsage(committed);
        nonRecursiveMulticellStatistics = SubtractAccountMulticellStatistics(
            account->MulticellStatistics(),
            account->ComputeTotalChildrenMulticellStatistics());
        clusterResources = &nonRecursiveResourceUsage;
        multicellStatistics = &nonRecursiveMulticellStatistics;
    }

    YT_ASSERT(clusterResources);
    YT_ASSERT(multicellStatistics);

    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();

    // Zero disk space usage should nevertheless be serialized if there's a
    // non-zero limit for that medium.
    TCompactVector<int, 4> additionalMediumIndexes;
    for (auto [mediumIndex, _] : account->ClusterResourceLimits().DiskSpace()) {
        additionalMediumIndexes.push_back(mediumIndex);
    }

    const auto& multicellManager = bootstrap->GetMulticellManager();
    DoSerializeClusterResources(
        *clusterResources,
        // Secondary cells don't store statistics of other cells.
        multicellManager->IsPrimaryMaster() ? multicellStatistics : nullptr,
        committed,
        fluent,
        bootstrap,
        /*serializeTabletResources*/ true,
        additionalMediumIndexes);

    fluent
        .EndMap();
}

void DeserializeClusterResources(
    TClusterResources& clusterResources,
    NYTree::INodePtr node,
    const TBootstrap* bootstrap)
{
    DoDeserializeClusterResources(
        clusterResources,
        std::move(node),
        bootstrap,
        /*deserializeTabletResources*/ true);
}

void DeserializeRichClusterResources(
    TRichClusterResources& clusterResources,
    NYTree::INodePtr node,
    const TBootstrap* bootstrap)
{
    TRichClusterResources result;

    DoDeserializeClusterResources(
        result.ClusterResources,
        node,
        bootstrap,
        /*serializeTabletResources*/ false);

    auto map = node->AsMap();

    Deserialize(result.TabletResources, node);

    clusterResources = result;
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
    builder->AppendFormat("], NodeCount: %v, ChunkCount: %v, TabletCount: %v, TabletStaticMemory: %v, ChunkHostCellMasterMemory: %v, DetailedMasterMemory: %v}",
        resources.GetNodeCount(),
        resources.GetChunkCount(),
        resources.GetTabletCount(),
        resources.GetTabletStaticMemory(),
        resources.GetChunkHostCellMasterMemory(),
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
