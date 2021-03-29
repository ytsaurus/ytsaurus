#include "cluster_resources.h"
#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/ytree/yson_serializable.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NCellMaster;
using namespace NChunkServer;

using NChunkClient::MaxMediumCount;
using NChunkServer::DefaultStoreMediumIndex;

////////////////////////////////////////////////////////////////////////////////

TClusterResourceLimits::TClusterResourceLimits()
    : NodeCount(0)
    , ChunkCount(0)
    , TabletCount(0)
    , TabletStaticMemory(0)
    , MasterMemory(0)
    , ChunkHostMasterMemory(0)
    , DiskSpace_{}
    , CellMasterMemoryLimits_{}
{ }

TClusterResourceLimits&& TClusterResourceLimits::SetNodeCount(i64 nodeCount) &&
{
    NodeCount = nodeCount;
    return std::move(*this);
}

TClusterResourceLimits&& TClusterResourceLimits::SetChunkCount(i64 chunkCount) &&
{
    ChunkCount = chunkCount;
    return std::move(*this);
}

TClusterResourceLimits&& TClusterResourceLimits::SetTabletCount(int tabletCount) &&
{
    TabletCount = tabletCount;
    return std::move(*this);
}

TClusterResourceLimits&& TClusterResourceLimits::SetTabletStaticMemory(i64 tabletStaticMemory) &&
{
    TabletStaticMemory = tabletStaticMemory;
    return std::move(*this);
}

TClusterResourceLimits&& TClusterResourceLimits::SetMasterMemory(i64 masterMemory) &&
{
    MasterMemory = masterMemory;
    return std::move(*this);
}

TClusterResourceLimits&& TClusterResourceLimits::SetChunkHostMasterMemory(i64 chunkHostMasterMemory) &&
{
    ChunkHostMasterMemory = chunkHostMasterMemory;
    return std::move(*this);
}

void TClusterResourceLimits::SetMasterMemory(NObjectServer::TCellTag cellTag, i64 masterMemory)
{
    CellMasterMemoryLimits_[cellTag] = masterMemory;
}

void TClusterResourceLimits::AddMasterMemory(NObjectServer::TCellTag cellTag, i64 masterMemory)
{
    CellMasterMemoryLimits_[cellTag] += masterMemory;
}

void TClusterResourceLimits::RemoveCellMasterMemoryEntry(NObjectServer::TCellTag cellTag)
{
    CellMasterMemoryLimits_.erase(cellTag);
}

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

const THashMap<NObjectServer::TCellTag, i64>& TClusterResourceLimits::CellMasterMemoryLimits() const
{
    return CellMasterMemoryLimits_;
}

/*static*/ TClusterResourceLimits TClusterResourceLimits::Infinite()
{
    auto resources = TClusterResourceLimits()
        .SetNodeCount(std::numeric_limits<i64>::max() / 2)
        .SetTabletCount(std::numeric_limits<int>::max() / 2)
        .SetChunkCount(std::numeric_limits<i64>::max() / 2)
        .SetTabletStaticMemory(std::numeric_limits<i64>::max() / 2)
        .SetMasterMemory(std::numeric_limits<i64>::max() / 2)
        .SetChunkHostMasterMemory(std::numeric_limits<i64>::max() / 2);
    for (int mediumIndex = 0; mediumIndex < NChunkClient::MaxMediumCount; ++mediumIndex) {
        resources.SetMediumDiskSpace(mediumIndex, std::numeric_limits<i64>::max() / 2);
    }
    return resources;
}

/*static*/ TClusterResourceLimits TClusterResourceLimits::Zero(const TMulticellManagerPtr& multicellManager)
{
    auto resources = TClusterResourceLimits();
    for (auto cellTag : multicellManager->GetSecondaryCellTags()) {
        resources.SetMasterMemory(cellTag, 0);
    }
    resources.SetMasterMemory(multicellManager->GetPrimaryCellTag(), 0);
    return resources;
}

void TClusterResourceLimits::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DiskSpace_);
    Save(context, NodeCount);
    Save(context, ChunkCount);
    Save(context, TabletCount);
    Save(context, TabletStaticMemory);
    Save(context, MasterMemory);
    Save(context, ChunkHostMasterMemory);
    Save(context, CellMasterMemoryLimits_);
}

void TClusterResourceLimits::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, DiskSpace_);
    Load(context, NodeCount);
    Load(context, ChunkCount);
    Load(context, TabletCount);
    Load(context, TabletStaticMemory);
    Load(context, MasterMemory);
    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= NCellMaster::EMasterReign::PerCellPerRoleMasterMemoryLimit) {
        Load(context, ChunkHostMasterMemory);
        Load(context, CellMasterMemoryLimits_);
    }
}

void TClusterResourceLimits::Save(NCypressServer::TBeginCopyContext& context) const
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
    Save(context, ChunkHostMasterMemory);
    Save(context, CellMasterMemoryLimits_);
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
    Load(context, NodeCount);
    Load(context, ChunkCount);
    Load(context, TabletCount);
    Load(context, TabletStaticMemory);
    Load(context, MasterMemory);
    Load(context, ChunkHostMasterMemory);
    Load(context, CellMasterMemoryLimits_);
}

bool TClusterResourceLimits::IsViolatedBy(const TClusterResourceLimits& rhs) const
{
    if (this == &rhs) {
        return false;
    }

    for (auto [mediumIndex, lhsDiskSpace] : DiskSpace()) {
        auto rhsDiskSpace = rhs.DiskSpace().lookup(mediumIndex);
        if (lhsDiskSpace < rhsDiskSpace) {
            return true;
        }
    }

    for (auto [mediumIndex, rhsDiskSpace] : rhs.DiskSpace()) {
        auto lhsDiskSpace = DiskSpace().lookup(mediumIndex);
        if (lhsDiskSpace < rhsDiskSpace) {
            return true;
        }
    }

    const auto& rhsCellMasterMemoryLimits = rhs.CellMasterMemoryLimits();
    for (auto [cellTag, lhsMasterMemory] : CellMasterMemoryLimits()) {
        auto rhsMasterMemoryIt = rhsCellMasterMemoryLimits.find(cellTag);
        // No entry means there is no limit (master memory for the given cell tag is unlimited).
        if (rhsMasterMemoryIt == rhsCellMasterMemoryLimits.end() || lhsMasterMemory < rhsMasterMemoryIt->second) {
            return true;
        }
    }

    return
        NodeCount < rhs.NodeCount ||
        ChunkCount < rhs.ChunkCount ||
        TabletCount < rhs.TabletCount ||
        TabletStaticMemory < rhs.TabletStaticMemory ||
        MasterMemory < rhs.MasterMemory ||
        ChunkHostMasterMemory < rhs.ChunkHostMasterMemory;
}


TClusterResourceLimits::TViolatedResourceLimits TClusterResourceLimits::GetViolatedBy(
    const TClusterResourceLimits& rhs) const
{
    if (this == &rhs) {
        return {};
    }

    auto result = TViolatedResourceLimits()
        .SetNodeCount(NodeCount < rhs.NodeCount)
        .SetChunkCount(ChunkCount < rhs.ChunkCount)
        .SetTabletCount(TabletCount < rhs.TabletCount)
        .SetTabletStaticMemory(TabletStaticMemory < rhs.TabletStaticMemory)
        .SetMasterMemory(MasterMemory < rhs.MasterMemory)
        .SetChunkHostMasterMemory(ChunkHostMasterMemory < rhs.ChunkHostMasterMemory);

    for (auto [mediumIndex, diskSpace] : DiskSpace()) {
        auto usageDiskSpace = rhs.DiskSpace().lookup(mediumIndex);
        if (diskSpace < usageDiskSpace) {
            result.SetMediumDiskSpace(mediumIndex, 1);
        }
    }

    for (auto [mediumIndex, usageDiskSpace] : rhs.DiskSpace()) {
        auto diskSpace = DiskSpace().lookup(mediumIndex);
        if (diskSpace < usageDiskSpace) {
            result.SetMediumDiskSpace(mediumIndex, 1);
        }
    }

    const auto& usageCellMasterMemory = rhs.CellMasterMemoryLimits();
    for (auto [cellTag, masterMemory] : CellMasterMemoryLimits()) {
        auto masterMemoryIt = usageCellMasterMemory.find(cellTag);
        if (masterMemoryIt == usageCellMasterMemory.end() || masterMemory < masterMemoryIt->second) {
            result.SetMasterMemory(cellTag, 1);
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////
TSerializableClusterResourceLimits::TSerializableMasterMemoryLimits::TSerializableMasterMemoryLimits()
{
    RegisterParameter("total", Total)
        .GreaterThanOrEqual(0)
        .Optional();
    RegisterParameter("chunk_host", ChunkHost)
        .GreaterThanOrEqual(0)
        .Optional();
    RegisterParameter("per_cell", PerCell)
        .Optional();
}

TSerializableClusterResourceLimits::TSerializableClusterResourceLimits(bool serializeDiskSpace)
{
    RegisterParameter("node_count", NodeCount_)
        .Default(0)
        .GreaterThanOrEqual(0);
    RegisterParameter("chunk_count", ChunkCount_)
        .Default(0)
        .GreaterThanOrEqual(0);
    RegisterParameter("tablet_count", TabletCount_)
        .Default(0)
        .GreaterThanOrEqual(0);
    RegisterParameter("tablet_static_memory", TabletStaticMemory_)
        .Default(0)
        .GreaterThanOrEqual(0);
    RegisterParameter("disk_space_per_medium", DiskSpacePerMedium_)
        .Optional();
    // NB: this is for (partial) compatibility: 'disk_space' is serialized when
    // read, but ignored when set. Hence no validation.
    if (serializeDiskSpace) {
        RegisterParameter("disk_space", DiskSpace_)
            .Optional();
    }

    RegisterParameter("master_memory", MasterMemory_)
        .Optional()
        .DefaultNew();

    RegisterPostprocessor([&] {
        for (auto [medium, diskSpace] : DiskSpacePerMedium_) {
            ValidateDiskSpace(diskSpace);
        }

        for (const auto [cellName, masterMemory] : MasterMemory_->PerCell) {
            if (masterMemory < 0) {
                THROW_ERROR_EXCEPTION("Invalid cell master memory for %v cell: expected >= 0, found %v",
                    cellName,
                    masterMemory);
            }
        }
    });
}

TSerializableClusterResourceLimits::TSerializableClusterResourceLimits(
    const TChunkManagerPtr& chunkManager,
    const TMulticellManagerPtr& multicellManager,
    const TClusterResourceLimits& clusterResourceLimits,
    bool serializeDiskSpace)
    : TSerializableClusterResourceLimits(serializeDiskSpace)
{
    NodeCount_ = clusterResourceLimits.NodeCount;
    ChunkCount_ = clusterResourceLimits.ChunkCount;
    TabletCount_ = clusterResourceLimits.TabletCount;
    TabletStaticMemory_ = clusterResourceLimits.TabletStaticMemory;
    MasterMemory_->Total = clusterResourceLimits.MasterMemory;
    MasterMemory_->ChunkHost = clusterResourceLimits.ChunkHostMasterMemory;
    DiskSpace_ = 0;
    for (auto [mediumIndex, mediumDiskSpace] : clusterResourceLimits.DiskSpace()) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || medium->GetCache()) {
            continue;
        }
        YT_VERIFY(DiskSpacePerMedium_.emplace(medium->GetName(), mediumDiskSpace).second);
        if (serializeDiskSpace) {
            DiskSpace_ += mediumDiskSpace;
        }
    }
    for (auto [cellTag, masterMemory] : clusterResourceLimits.CellMasterMemoryLimits()) {
        auto cellName = multicellManager->GetMasterCellName(cellTag);
        YT_VERIFY(MasterMemory_->PerCell.emplace(cellName, masterMemory).second);
    }
}

TClusterResourceLimits TSerializableClusterResourceLimits::ToClusterResourceLimits(
    const TChunkManagerPtr& chunkManager,
    const TMulticellManagerPtr& multicellManager) const
{
    auto result = TClusterResourceLimits()
        .SetNodeCount(NodeCount_)
        .SetChunkCount(ChunkCount_)
        .SetTabletCount(TabletCount_)
        .SetTabletStaticMemory(TabletStaticMemory_)
        .SetMasterMemory(MasterMemory_->Total)
        .SetChunkHostMasterMemory(MasterMemory_->ChunkHost);
    for (auto [mediumName, mediumDiskSpace] : DiskSpacePerMedium_) {
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        result.SetMediumDiskSpace(medium->GetIndex(), mediumDiskSpace);
    }
    for (const auto& [cellName, masterMemory] : MasterMemory_->PerCell) {
        auto optionalCellTag = multicellManager->FindMasterCellTagByName(cellName);
        if (optionalCellTag) {
            result.SetMasterMemory(*optionalCellTag, masterMemory);
        }
    }
    return result;
}

void TSerializableClusterResourceLimits::AddToMediumDiskSpace(const TString& mediumName, i64 mediumDiskSpace)
{
    DiskSpacePerMedium_[mediumName] += mediumDiskSpace;
}

////////////////////////////////////////////////////////////////////////////////

TClusterResourceLimits& operator += (TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs)
{
    for (auto [mediumIndex, diskSpace] : rhs.DiskSpace()) {
        lhs.AddToMediumDiskSpace(mediumIndex, diskSpace);
    }
    lhs.NodeCount += rhs.NodeCount;
    lhs.ChunkCount += rhs.ChunkCount;
    lhs.TabletCount += rhs.TabletCount;
    lhs.TabletStaticMemory += rhs.TabletStaticMemory;
    lhs.MasterMemory += rhs.MasterMemory;
    lhs.ChunkHostMasterMemory += rhs.ChunkHostMasterMemory;

    // No entry means there is no limit (the limit is infinite), so if either rhs or lhs does not have
    // an entry, the result should not have it as well.
    const auto& rhsCellMasterMemoryLimits = rhs.CellMasterMemoryLimits();
    for (auto [cellTag, masterMemory] : lhs.CellMasterMemoryLimits()) {
        auto it = rhsCellMasterMemoryLimits.find(cellTag);
        if (it != rhsCellMasterMemoryLimits.end()) {
            lhs.AddMasterMemory(cellTag, it->second);
        } else {
            lhs.RemoveCellMasterMemoryEntry(cellTag);
        }
    }

    return lhs;
}

TClusterResourceLimits operator + (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TClusterResourceLimits& operator -= (TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs)
{
    for (auto [mediumIndex, diskSpace] : rhs.DiskSpace()) {
        lhs.AddToMediumDiskSpace(mediumIndex, -diskSpace);
    }
    lhs.NodeCount -= rhs.NodeCount;
    lhs.ChunkCount -= rhs.ChunkCount;
    lhs.TabletCount -= rhs.TabletCount;
    lhs.TabletStaticMemory -= rhs.TabletStaticMemory;
    lhs.MasterMemory -= rhs.MasterMemory;
    lhs.ChunkHostMasterMemory -= rhs.ChunkHostMasterMemory;

    const auto& rhsCellMasterMemoryLimits = rhs.CellMasterMemoryLimits();
    for (auto [cellTag, masterMemory] : lhs.CellMasterMemoryLimits()) {
        auto it = rhsCellMasterMemoryLimits.find(cellTag);
        if (it != rhsCellMasterMemoryLimits.end()) {
            lhs.AddMasterMemory(cellTag, -it->second);
        } else {
            // We are actually subtracting infinity from a finite number.
            // The result does not matter, it does not make any sense anyway.
            lhs.RemoveCellMasterMemoryEntry(cellTag);
        }
    }
    for (auto [cellTag, masterMemory] : rhs.CellMasterMemoryLimits()) {
        lhs.AddMasterMemory(cellTag, -masterMemory);
    }
    return lhs;
}

TClusterResourceLimits operator - (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TClusterResourceLimits& operator *= (TClusterResourceLimits& lhs, i64 rhs)
{
    for (auto [mediumIndex, diskSpace] : lhs.DiskSpace()) {
        lhs.SetMediumDiskSpace(mediumIndex, diskSpace * rhs);
    }
    lhs.NodeCount *= rhs;
    lhs.ChunkCount *= rhs;
    lhs.TabletCount *= rhs;
    lhs.TabletStaticMemory *= rhs;
    lhs.MasterMemory *= rhs;
    lhs.ChunkHostMasterMemory *= rhs;
    for (auto [cellTag, masterMemory] : lhs.CellMasterMemoryLimits()) {
        lhs.SetMasterMemory(cellTag, masterMemory * rhs);
    }
    return lhs;
}

TClusterResourceLimits operator * (const TClusterResourceLimits& lhs, i64 rhs)
{
    auto result = lhs;
    result *= rhs;
    return result;
}

TClusterResourceLimits operator - (const TClusterResourceLimits& resources)
{
    TClusterResourceLimits result;
    for (auto [mediumIndex, diskSpace] : resources.DiskSpace()) {
        result.SetMediumDiskSpace(mediumIndex, -diskSpace);
    }
    result.NodeCount = -resources.NodeCount;
    result.ChunkCount = -resources.ChunkCount;
    result.TabletCount = -resources.TabletCount;
    result.TabletStaticMemory = -resources.TabletStaticMemory;
    result.MasterMemory = -resources.MasterMemory;
    result.ChunkHostMasterMemory = -resources.ChunkHostMasterMemory;
    for (auto [cellTag, masterMemory] : resources.CellMasterMemoryLimits()) {
        result.SetMasterMemory(cellTag, -masterMemory);
    }
    return result;
}

bool operator == (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs)
{
    if (&lhs == &rhs) {
        return true;
    }
    if (lhs.DiskSpace().size() != rhs.DiskSpace().size()) {
        return false;
    }
    for (auto [mediumIndex, mediumDiskSpace] : lhs.DiskSpace()) {
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
    if (lhs.ChunkHostMasterMemory != rhs.ChunkHostMasterMemory) {
        return false;
    }
    for (auto [cellTag, masterMemory] : lhs.CellMasterMemoryLimits()) {
        const auto it = rhs.CellMasterMemoryLimits().find(cellTag);
        if (it == rhs.CellMasterMemoryLimits().end() || it->second != masterMemory) {
            return false;
        }
    }
    return true;
}

bool operator != (const TClusterResourceLimits& lhs, const TClusterResourceLimits& rhs)
{
    return !(lhs == rhs);
}

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
    builder->AppendFormat("], NodeCount: %v, ChunkCount: %v, TabletCount: %v, TabletStaticMemory: %v, MasterMemory: %v, ChunkHostMasterMemory: %v, ",
        resources.NodeCount,
        resources.ChunkCount,
        resources.TabletCount,
        resources.TabletStaticMemory,
        resources.MasterMemory,
        resources.ChunkHostMasterMemory);

    builder->AppendFormat("PerCellMasterMemory: %v",
        resources.CellMasterMemoryLimits());
}

TString ToString(const TClusterResourceLimits& resources)
{
    return ToStringViaBuilder(resources);
}

////////////////////////////////////////////////////////////////////////////////

TSerializableViolatedClusterResourceLimits::TSerializableViolatedMasterMemoryLimits::TSerializableViolatedMasterMemoryLimits()
{
    RegisterParameter("total", Total)
        .Optional();
    RegisterParameter("chunk_host", ChunkHost)
        .Optional();
    RegisterParameter("per_cell", PerCell)
        .Optional();
}

TSerializableViolatedClusterResourceLimits::TSerializableViolatedClusterResourceLimits(
    const TChunkManagerPtr& chunkManager,
    const TMulticellManagerPtr& multicellManager,
    const TClusterResourceLimits& violatedResourceLimits)
{
    RegisterParameter("node_count", NodeCount_);
    RegisterParameter("chunk_count", ChunkCount_);
    RegisterParameter("tablet_count", TabletCount_);
    RegisterParameter("tablet_static_memory", TabletStaticMemory_);
    RegisterParameter("disk_space_per_medium", DiskSpacePerMedium_);
    RegisterParameter("master_memory", MasterMemory_)
        .Optional()
        .DefaultNew();

    NodeCount_ = violatedResourceLimits.NodeCount;
    ChunkCount_ = violatedResourceLimits.ChunkCount;
    TabletCount_ = violatedResourceLimits.TabletCount;
    TabletStaticMemory_ = violatedResourceLimits.TabletStaticMemory;
    MasterMemory_->Total = violatedResourceLimits.MasterMemory;
    MasterMemory_->ChunkHost = violatedResourceLimits.ChunkHostMasterMemory;

    for (auto [mediumIndex, mediumDiskSpace] : violatedResourceLimits.DiskSpace()) {
        const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
        if (!medium || medium->GetCache()) {
            continue;
        }
        YT_VERIFY(DiskSpacePerMedium_.emplace(medium->GetName(), mediumDiskSpace).second);
    }

    for (auto [cellTag, masterMemory] : violatedResourceLimits.CellMasterMemoryLimits()) {
        auto cellName = multicellManager->GetMasterCellName(cellTag);
        YT_VERIFY(MasterMemory_->PerCell.emplace(cellName, masterMemory > 0).second);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

