#include "cluster_resources.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;
using namespace NCellMaster;
using namespace NChunkServer;

////////////////////////////////////////////////////////////////////////////////

TClusterResourceLimits&& TClusterResourceLimits::SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &&
{
    SetMediumDiskSpace(mediumIndex, diskSpace);
    return std::move(*this);
}

void TClusterResourceLimits::SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &
{
    if (diskSpace == 0) {
        DiskSpace_.erase(mediumIndex);
    } else {
        DiskSpace_[mediumIndex] = diskSpace;
    }
}

void TClusterResourceLimits::AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta)
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

const NChunkClient::TMediumMap<i64>& TClusterResourceLimits::DiskSpace() const
{
    return DiskSpace_;
}

TMasterMemoryLimits& TClusterResourceLimits::MasterMemory()
{
    return MasterMemory_;
}

const TMasterMemoryLimits& TClusterResourceLimits::MasterMemory() const
{
    return MasterMemory_;
}

TClusterResourceLimits&& TClusterResourceLimits::SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &&
{
    MasterMemory_ = std::move(masterMemoryLimits);
    return std::move(*this);
}

void TClusterResourceLimits::SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &
{
    MasterMemory_ = std::move(masterMemoryLimits);
}

/*static*/ TClusterResourceLimits TClusterResourceLimits::Infinite()
{
    auto resources = TClusterResourceLimits()
        .SetNodeCount(std::numeric_limits<i64>::max() / 2)
        .SetTabletCount(std::numeric_limits<int>::max() / 2)
        .SetChunkCount(std::numeric_limits<i64>::max() / 2)
        .SetTabletStaticMemory(std::numeric_limits<i64>::max() / 2)
        .SetMasterMemory(TMasterMemoryLimits(
            /* total */ std::numeric_limits<i64>::max() / 2,
            /* chunkHost */ std::numeric_limits<i64>::max() / 2,
            /* perCell */ {}));
    for (int mediumIndex = 0; mediumIndex < NChunkClient::MaxMediumCount; ++mediumIndex) {
        resources.SetMediumDiskSpace(mediumIndex, std::numeric_limits<i64>::max() / 2);
    }
    return resources;
}

/*static*/ TClusterResourceLimits TClusterResourceLimits::Zero(const IMulticellManagerPtr& multicellManager)
{
    auto resources = TClusterResourceLimits();
    for (auto cellTag : multicellManager->GetSecondaryCellTags()) {
        resources.MasterMemory().PerCell[cellTag] = 0;
    }
    resources.MasterMemory().PerCell[multicellManager->GetPrimaryCellTag()] = 0;
    return resources;
}

void TClusterResourceLimits::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DiskSpace_);
    Save(context, NodeCount_);
    Save(context, ChunkCount_);
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
    Save(context, MasterMemory_);
}

void TClusterResourceLimits::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, DiskSpace_);
    Load(context, NodeCount_);
    Load(context, ChunkCount_);
    Load(context, TabletCount_);
    Load(context, TabletStaticMemory_);
    Load(context, MasterMemory_);
}

void TClusterResourceLimits::Save(NCypressServer::TBeginCopyContext& context) const
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
    Save(context, MasterMemory_);
}

void TClusterResourceLimits::Load(NCypressServer::TEndCopyContext& context)
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
    Load(context, MasterMemory_);
}

bool TClusterResourceLimits::IsViolatedBy(const TClusterResourceLimits& rhs) const
{
    if (this == &rhs) {
        return false;
    }

    for (auto [mediumIndex, lhsDiskSpace] : DiskSpace()) {
        auto rhsDiskSpace = GetOrDefault(rhs.DiskSpace(), mediumIndex);
        if (lhsDiskSpace < rhsDiskSpace) {
            return true;
        }
    }

    for (auto [mediumIndex, rhsDiskSpace] : rhs.DiskSpace()) {
        auto lhsDiskSpace = GetOrDefault(DiskSpace(), mediumIndex);
        if (lhsDiskSpace < rhsDiskSpace) {
            return true;
        }
    }

    return
        NodeCount_ < rhs.NodeCount_ ||
        ChunkCount_ < rhs.ChunkCount_ ||
        TabletCount_ < rhs.TabletCount_ ||
        TabletStaticMemory_ < rhs.TabletStaticMemory_ ||
        MasterMemory_.IsViolatedBy(rhs.MasterMemory());
}

TViolatedClusterResourceLimits TClusterResourceLimits::GetViolatedBy(
    const TClusterResourceLimits& rhs) const
{
    if (this == &rhs) {
        return {};
    }

    TViolatedClusterResourceLimits result;
    result.SetNodeCount(NodeCount_ < rhs.NodeCount_);
    result.SetChunkCount(ChunkCount_ < rhs.ChunkCount_);
    result.SetTabletCount(TabletCount_ < rhs.TabletCount_);
    result.SetTabletStaticMemory(TabletStaticMemory_ < rhs.TabletStaticMemory_);
    result.SetMasterMemory(MasterMemory_.GetViolatedBy(rhs.MasterMemory_));

    for (auto [mediumIndex, diskSpace] : DiskSpace()) {
        auto usageDiskSpace = GetOrDefault(rhs.DiskSpace(), mediumIndex);
        if (diskSpace < usageDiskSpace) {
            result.SetMediumDiskSpace(mediumIndex, diskSpace < usageDiskSpace);
        }
    }

    for (auto [mediumIndex, usageDiskSpace] : rhs.DiskSpace()) {
        auto diskSpace = GetOrDefault(DiskSpace(), mediumIndex);
        if (diskSpace < usageDiskSpace) {
            result.SetMediumDiskSpace(mediumIndex, diskSpace < usageDiskSpace);
        }
    }

    return result;
}

TClusterResourceLimits& TClusterResourceLimits::operator += (const TClusterResourceLimits& other)
{
    for (auto [mediumIndex, diskSpace] : other.DiskSpace()) {
        AddToMediumDiskSpace(mediumIndex, diskSpace);
    }
    NodeCount_ += other.NodeCount_;
    ChunkCount_ += other.ChunkCount_;
    TabletCount_ += other.TabletCount_;
    TabletStaticMemory_ += other.TabletStaticMemory_;
    MasterMemory_ += other.MasterMemory_;

    return *this;
}

TClusterResourceLimits TClusterResourceLimits::operator + (const TClusterResourceLimits& other) const
{
    auto result = *this;
    result += other;
    return result;
}

TClusterResourceLimits& TClusterResourceLimits::operator -= (const TClusterResourceLimits& other)
{
    for (auto [mediumIndex, diskSpace] : other.DiskSpace()) {
        AddToMediumDiskSpace(mediumIndex, -diskSpace);
    }
    NodeCount_ -= other.NodeCount_;
    ChunkCount_ -= other.ChunkCount_;
    TabletCount_ -= other.TabletCount_;
    TabletStaticMemory_ -= other.TabletStaticMemory_;
    MasterMemory_ -= other.MasterMemory_;

    return *this;
}

TClusterResourceLimits TClusterResourceLimits::operator - (const TClusterResourceLimits& other) const
{
    auto result = *this;
    result -= other;
    return result;
}

TClusterResourceLimits& TClusterResourceLimits::operator *= (i64 other)
{
    for (auto [mediumIndex, diskSpace] : DiskSpace()) {
        SetMediumDiskSpace(mediumIndex, diskSpace * other);
    }
    NodeCount_ *= other;
    ChunkCount_ *= other;
    TabletCount_ *= other;
    TabletStaticMemory_ *= other;
    MasterMemory_ *= other;
    return *this;
}

TClusterResourceLimits TClusterResourceLimits::operator * (i64 other) const
{
    auto result = *this;
    result *= other;
    return result;
}

TClusterResourceLimits TClusterResourceLimits::operator - () const
{
    TClusterResourceLimits result;
    for (auto [mediumIndex, diskSpace] : DiskSpace()) {
        result.SetMediumDiskSpace(mediumIndex, -diskSpace);
    }
    result.NodeCount_ = -NodeCount_;
    result.ChunkCount_ = -ChunkCount_;
    result.TabletCount_ = -TabletCount_;
    result.TabletStaticMemory_ = -TabletStaticMemory_;
    result.MasterMemory_ = -MasterMemory_;
    return result;
}

bool TClusterResourceLimits::operator == (const TClusterResourceLimits& other) const
{
    if (this == &other) {
        return true;
    }
    if (DiskSpace().size() != other.DiskSpace().size()) {
        return false;
    }
    for (auto [mediumIndex, mediumDiskSpace] : DiskSpace()) {
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
    if (MasterMemory_ != other.MasterMemory_) {
        return false;
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

TMasterMemoryLimits& TViolatedClusterResourceLimits::MasterMemory()
{
    return MasterMemory_;
}

const TMasterMemoryLimits& TViolatedClusterResourceLimits::MasterMemory() const
{
    return MasterMemory_;
}

void TViolatedClusterResourceLimits::SetMasterMemory(TMasterMemoryLimits masterMemoryLimits) &
{
    MasterMemory_ = std::move(masterMemoryLimits);
}

NChunkClient::TMediumMap<i64>& TViolatedClusterResourceLimits::DiskSpace()
{
    return DiskSpace_;
}

const TMediumMap<i64>& TViolatedClusterResourceLimits::DiskSpace() const
{
    return DiskSpace_;
}

void TViolatedClusterResourceLimits::SetMediumDiskSpace(int mediumIndex, i64 diskSpace) &
{
    DiskSpace_[mediumIndex] = diskSpace;
}

void TViolatedClusterResourceLimits::AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta)
{
    DiskSpace_[mediumIndex] += diskSpaceDelta;
}

void SerializeClusterResourceLimits(
    const TClusterResourceLimits& resourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap,
    bool serializeDiskSpace)
{
    const auto& chunkManager = bootstrap->GetChunkManager();
    const auto& multicellManager = bootstrap->GetMulticellManager();

    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();
    fluent
        .Item("node_count").Value(resourceLimits.GetNodeCount())
        .Item("chunk_count").Value(resourceLimits.GetChunkCount())
        .Item("tablet_count").Value(resourceLimits.GetTabletCount())
        .Item("tablet_static_memory").Value(resourceLimits.GetTabletStaticMemory())
        .Item("disk_space_per_medium").DoMapFor(
            resourceLimits.DiskSpace(),
            [&] (TFluentMap fluent, auto pair) {
                auto [mediumIndex, mediumDiskSpace] = pair;
                const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                if (!IsObjectAlive(medium)) {
                    return;
                }
                fluent.Item(medium->GetName()).Value(mediumDiskSpace);
            })
        .DoIf(serializeDiskSpace, [&] (TFluentMap fluent) {
            fluent
                .Item("disk_space").Value(std::accumulate(
                    resourceLimits.DiskSpace().begin(),
                    resourceLimits.DiskSpace().end(),
                    i64(0),
                    [&] (i64 totalDiskSpace, auto pair) {
                        auto [mediumIndex, mediumDiskSpace] = pair;
                        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                        if (!IsObjectAlive(medium)) {
                            return totalDiskSpace;
                        }
                        return totalDiskSpace + mediumDiskSpace;
                    }));
        })
        .Item("master_memory");
    SerializeMasterMemoryLimits(
        resourceLimits.MasterMemory(),
        fluent.GetConsumer(),
        multicellManager);
    fluent
        .EndMap();
}

void DeserializeClusterResourceLimits(
    TClusterResourceLimits& resourceLimits,
    NYTree::INodePtr node,
    const NCellMaster::TBootstrap* bootstrap)
{
    TClusterResourceLimits result;

    auto map = node->AsMap();

    result.SetNodeCount(GetOptionalNonNegativeI64ChildOrThrow(map, "node_count"));
    result.SetChunkCount(GetOptionalNonNegativeI64ChildOrThrow(map, "chunk_count"));
    result.SetTabletCount(GetOptionalNonNegativeI64ChildOrThrow(map, "tablet_count"));
    result.SetTabletStaticMemory(GetOptionalNonNegativeI64ChildOrThrow(map, "tablet_static_memory"));

    const auto& chunkManager = bootstrap->GetChunkManager();
    if (auto diskSpacePerMediumNode = map->FindChild("disk_space_per_medium")) {
        for (const auto& [mediumName, mediumDiskSpaceNode] : diskSpacePerMediumNode->AsMap()->GetChildren()) {
            auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
            auto mediumDiskSpace = mediumDiskSpaceNode->AsInt64()->GetValue();
            ValidateDiskSpace(mediumDiskSpace);
            result.SetMediumDiskSpace(medium->GetIndex(), mediumDiskSpace);
        }
    }

    const auto& multicellManager = bootstrap->GetMulticellManager();
    if (auto masterMemoryNode = map->FindChild("master_memory")) {
        DeserializeMasterMemoryLimits(result.MasterMemory(), masterMemoryNode, multicellManager);
    }

    resourceLimits = std::move(result);
}

void SerializeViolatedClusterResourceLimits(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const TBootstrap* bootstrap)
{
    const auto& chunkManager = bootstrap->GetChunkManager();
    const auto& multicellManager = bootstrap->GetMulticellManager();

    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();
    fluent
        .Item("node_count").Value(violatedResourceLimits.GetNodeCount())
        .Item("chunk_count").Value(violatedResourceLimits.GetChunkCount())
        .Item("tablet_count").Value(violatedResourceLimits.GetTabletCount())
        .Item("tablet_static_memory").Value(violatedResourceLimits.GetTabletStaticMemory())
        .Item("disk_space_per_medium").DoMapFor(
            violatedResourceLimits.DiskSpace(),
            [&] (TFluentMap fluent, auto pair) {
                auto [mediumIndex, mediumDiskSpace] = pair;
                const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                if (!IsObjectAlive(medium)) {
                    return;
                }
                fluent.Item(medium->GetName()).Value(mediumDiskSpace);
            })
        .Item("master_memory");

    SerializeViolatedMasterMemoryLimits(
        violatedResourceLimits.MasterMemory(),
        fluent.GetConsumer(),
        multicellManager);

    fluent
        .EndMap();
}

void SerializeViolatedClusterResourceLimitsInCompactFormat(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap)
{
    const auto& chunkManager = bootstrap->GetChunkManager();
    const auto& multicellManager = bootstrap->GetMulticellManager();

    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();
    fluent
        .DoIf(violatedResourceLimits.GetNodeCount() > 0, [&] (TFluentMap fluent) {
            fluent.Item("node_count").Value(violatedResourceLimits.GetNodeCount());
        })
        .DoIf(violatedResourceLimits.GetChunkCount() > 0, [&] (TFluentMap fluent) {
            fluent.Item("chunk_count").Value(violatedResourceLimits.GetChunkCount());
        })
        .DoIf(violatedResourceLimits.GetTabletCount() > 0, [&] (TFluentMap fluent) {
            fluent.Item("tablet_count").Value(violatedResourceLimits.GetTabletCount());
        })
        .DoIf(violatedResourceLimits.GetTabletStaticMemory() > 0, [&] (TFluentMap fluent) {
            fluent.Item("tablet_static_memory").Value(violatedResourceLimits.GetTabletStaticMemory());
        });

    bool diskSpaceLimitViolated = false;
    for (auto [mediumIndex, mediumDiskSpace] : violatedResourceLimits.DiskSpace()) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!IsObjectAlive(medium) || mediumDiskSpace == 0) {
            continue;
        }
        diskSpaceLimitViolated = true;
        break;
    }

    fluent
        .DoIf(diskSpaceLimitViolated, [&] (TFluentMap fluent) {
            fluent.Item("disk_space_per_medium").DoMapFor(
                violatedResourceLimits.DiskSpace(),
                [&] (TFluentMap fluent, auto pair) {
                    auto [mediumIndex, mediumDiskSpace] = pair;
                    const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                    if (!IsObjectAlive(medium) || mediumDiskSpace == 0) {
                        return;
                    }
                    fluent.Item(medium->GetName()).Value(mediumDiskSpace);
                });
        });

    if (violatedResourceLimits.MasterMemory().Total > 0 ||
        violatedResourceLimits.MasterMemory().ChunkHost > 0 ||
        !violatedResourceLimits.MasterMemory().PerCell.empty())
    {
        fluent
            .Item("master_memory");
        SerializeViolatedMasterMemoryLimits(
            violatedResourceLimits.MasterMemory(),
            fluent.GetConsumer(),
            multicellManager);
    }

    fluent
        .EndMap();
}

void SerializeViolatedClusterResourceLimitsInBooleanFormat(
    const TViolatedClusterResourceLimits& violatedResourceLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap,
    bool serializeDiskSpace)
{
    const auto& chunkManager = bootstrap->GetChunkManager();
    const auto& multicellManager = bootstrap->GetMulticellManager();

    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();
    fluent
        .Item("node_count").Value(violatedResourceLimits.GetNodeCount() != 0)
        .Item("chunk_count").Value(violatedResourceLimits.GetChunkCount() != 0)
        .Item("tablet_count").Value(violatedResourceLimits.GetTabletCount() != 0)
        .Item("tablet_static_memory").Value(violatedResourceLimits.GetTabletStaticMemory() != 0)
        .Item("disk_space_per_medium").DoMapFor(
            violatedResourceLimits.DiskSpace(),
            [&] (TFluentMap fluent, auto pair) {
                auto [mediumIndex, mediumDiskSpace] = pair;
                const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                if (IsObjectAlive(medium)) {
                    fluent.Item(medium->GetName()).Value(mediumDiskSpace != 0);
                }
            })
        .DoIf(serializeDiskSpace, [&] (TFluentMap fluent) {
            fluent
                .Item("disk_space").Value(std::any_of(
                    violatedResourceLimits.DiskSpace().begin(),
                    violatedResourceLimits.DiskSpace().end(),
                    [&] (auto pair) {
                        auto [mediumIndex, mediumDiskSpace] = pair;
                        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                        if (!IsObjectAlive(medium)) {
                            return false;
                        }
                        return mediumDiskSpace != 0;
                    }));
        })
        .Item("master_memory");

    SerializeViolatedMasterMemoryLimitsInBooleanFormat(
        violatedResourceLimits.MasterMemory(),
        fluent.GetConsumer(),
        multicellManager);

    fluent
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TClusterResourceLimits& resources, TStringBuf /*format*/)
{
    builder->AppendString(TStringBuf("{DiskSpace: ["));
    auto firstDiskSpace = true;
    for (auto [mediumIndex, diskSpace] : resources.DiskSpace()) {
        if (diskSpace != 0) {
            if (!firstDiskSpace) {
                builder->AppendString(TStringBuf(", "));
            }
            builder->AppendFormat("%v@%v", diskSpace, mediumIndex);
            firstDiskSpace = false;
        }
    }
    builder->AppendFormat("], NodeCount: %v, ChunkCount: %v, TabletCount: %v, TabletStaticMemory: %v, MasterMemory: %v",
        resources.GetNodeCount(),
        resources.GetChunkCount(),
        resources.GetTabletCount(),
        resources.GetTabletStaticMemory(),
        resources.MasterMemory());
}

TString ToString(const TClusterResourceLimits& resources)
{
    return ToStringViaBuilder(resources);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

