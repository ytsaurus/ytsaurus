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
    : NodeCount_(0)
    , ChunkCount_(0)
    , TabletCount_(0)
    , TabletStaticMemory_(0)
    , MasterMemory_(0)
    , ChunkHostMasterMemory_(0)
    , DiskSpace_{}
    , CellMasterMemoryLimits_{}
{ }

TClusterResourceLimits&& TClusterResourceLimits::SetMasterMemory(THashMap<NObjectServer::TCellTag, i64> masterMemory) &&
{
    CellMasterMemoryLimits_ = std::move(masterMemory);
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
    Save(context, NodeCount_);
    Save(context, ChunkCount_);
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
    Save(context, MasterMemory_);
    Save(context, ChunkHostMasterMemory_);
    Save(context, CellMasterMemoryLimits_);
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
    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= NCellMaster::EMasterReign::PerCellPerRoleMasterMemoryLimit) {
        Load(context, ChunkHostMasterMemory_);
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
    Save(context, NodeCount_);
    Save(context, ChunkCount_);
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
    Save(context, MasterMemory_);
    Save(context, ChunkHostMasterMemory_);
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
    Load(context, NodeCount_);
    Load(context, ChunkCount_);
    Load(context, TabletCount_);
    Load(context, TabletStaticMemory_);
    Load(context, MasterMemory_);
    Load(context, ChunkHostMasterMemory_);
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
        NodeCount_ < rhs.NodeCount_ ||
        ChunkCount_ < rhs.ChunkCount_ ||
        TabletCount_ < rhs.TabletCount_ ||
        TabletStaticMemory_ < rhs.TabletStaticMemory_ ||
        MasterMemory_ < rhs.MasterMemory_ ||
        ChunkHostMasterMemory_ < rhs.ChunkHostMasterMemory_;
}


TClusterResourceLimits::TViolatedResourceLimits TClusterResourceLimits::GetViolatedBy(
    const TClusterResourceLimits& rhs) const
{
    if (this == &rhs) {
        return {};
    }

    auto result = TViolatedResourceLimits()
        .SetNodeCount(NodeCount_ < rhs.NodeCount_)
        .SetChunkCount(ChunkCount_ < rhs.ChunkCount_)
        .SetTabletCount(TabletCount_ < rhs.TabletCount_)
        .SetTabletStaticMemory(TabletStaticMemory_ < rhs.TabletStaticMemory_)
        .SetMasterMemory(MasterMemory_ < rhs.MasterMemory_)
        .SetChunkHostMasterMemory(ChunkHostMasterMemory_ < rhs.ChunkHostMasterMemory_);

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
    ChunkHostMasterMemory_ += other.ChunkHostMasterMemory_;

    // No entry means there is no limit (the limit is infinite), so if either other or lhs does not have
    // an entry, the result should not have it as well.
    const auto& otherCellMasterMemoryLimits = other.CellMasterMemoryLimits();
    for (auto [cellTag, masterMemory] : CellMasterMemoryLimits()) {
        auto it = otherCellMasterMemoryLimits.find(cellTag);
        if (it != otherCellMasterMemoryLimits.end()) {
            AddMasterMemory(cellTag, it->second);
        } else {
            RemoveCellMasterMemoryEntry(cellTag);
        }
    }

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
    ChunkHostMasterMemory_ -= other.ChunkHostMasterMemory_;

    const auto& otherCellMasterMemoryLimits = other.CellMasterMemoryLimits();
    for (auto [cellTag, masterMemory] : CellMasterMemoryLimits()) {
        auto it = otherCellMasterMemoryLimits.find(cellTag);
        if (it != otherCellMasterMemoryLimits.end()) {
            AddMasterMemory(cellTag, -it->second);
        } else {
            // We are actually subtracting infinity from a finite number.
            // The result does not matter, it does not make any sense anyway.
            RemoveCellMasterMemoryEntry(cellTag);
        }
    }
    for (auto [cellTag, masterMemory] : other.CellMasterMemoryLimits()) {
        AddMasterMemory(cellTag, -masterMemory);
    }
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
    ChunkHostMasterMemory_ *= other;
    for (auto [cellTag, masterMemory] : CellMasterMemoryLimits()) {
        SetMasterMemory(cellTag, masterMemory * other);
    }
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
    result.ChunkHostMasterMemory_ = -ChunkHostMasterMemory_;
    for (auto [cellTag, masterMemory] : CellMasterMemoryLimits()) {
        result.SetMasterMemory(cellTag, -masterMemory);
    }
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
    if (ChunkHostMasterMemory_ != other.ChunkHostMasterMemory_) {
        return false;
    }
    for (auto [cellTag, masterMemory] : CellMasterMemoryLimits()) {
        const auto it = other.CellMasterMemoryLimits().find(cellTag);
        if (it == other.CellMasterMemoryLimits().end() || it->second != masterMemory) {
            return false;
        }
    }
    return true;
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
    NodeCount_ = clusterResourceLimits.GetNodeCount();
    ChunkCount_ = clusterResourceLimits.GetChunkCount();
    TabletCount_ = clusterResourceLimits.GetTabletCount();
    TabletStaticMemory_ = clusterResourceLimits.GetTabletStaticMemory();
    MasterMemory_->Total = clusterResourceLimits.GetMasterMemory();
    MasterMemory_->ChunkHost = clusterResourceLimits.GetChunkHostMasterMemory();
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
    MasterMemory_->PerCell = CellTagMapToCellNameMap(clusterResourceLimits.CellMasterMemoryLimits(), multicellManager);
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
        .SetChunkHostMasterMemory(MasterMemory_->ChunkHost)
        .SetMasterMemory(CellNameMapToCellTagMapOrThrow(MasterMemory_->PerCell, multicellManager));

    for (auto [mediumName, mediumDiskSpace] : DiskSpacePerMedium_) {
        auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
        result.SetMediumDiskSpace(medium->GetIndex(), mediumDiskSpace);
    }

    return result;
}

void TSerializableClusterResourceLimits::AddToMediumDiskSpace(const TString& mediumName, i64 mediumDiskSpace)
{
    DiskSpacePerMedium_[mediumName] += mediumDiskSpace;
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
    builder->AppendFormat("], NodeCount: %v, ChunkCount: %v, TabletCount: %v, TabletStaticMemory: %v, MasterMemory: %v, ChunkHostMasterMemory: %v, ",
        resources.GetNodeCount(),
        resources.GetChunkCount(),
        resources.GetTabletCount(),
        resources.GetTabletStaticMemory(),
        resources.GetMasterMemory(),
        resources.GetChunkHostMasterMemory());

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

    NodeCount_ = violatedResourceLimits.GetNodeCount();
    ChunkCount_ = violatedResourceLimits.GetChunkCount();
    TabletCount_ = violatedResourceLimits.GetTabletCount();
    TabletStaticMemory_ = violatedResourceLimits.GetTabletStaticMemory();
    MasterMemory_->Total = violatedResourceLimits.GetMasterMemory();
    MasterMemory_->ChunkHost = violatedResourceLimits.GetChunkHostMasterMemory();

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

