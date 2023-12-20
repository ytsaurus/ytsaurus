#include "cluster_resources.h"

#include "helpers.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/chunk_server/chunk_manager.h>
#include <yt/yt/server/master/chunk_server/medium_base.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/yt/yson/public.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;
using namespace NCellMaster;
using namespace NChunkServer;

using NObjectServer::TCellTag;

////////////////////////////////////////////////////////////////////////////////

TClusterResourceLimits&& TClusterResourceLimits::SetMediumDiskSpace(int mediumIndex, TLimit64  diskSpace) &&
{
    SetMediumDiskSpace(mediumIndex, diskSpace);
    return std::move(*this);
}

void TClusterResourceLimits::SetMediumDiskSpace(int mediumIndex, TLimit64  diskSpace) &
{
    if (diskSpace == TLimit64(0)) {
        DiskSpace_.erase(mediumIndex);
    } else {
        DiskSpace_[mediumIndex] = TLimit64(diskSpace);
    }
}

void TClusterResourceLimits::AddToMediumDiskSpace(int mediumIndex, i64 diskSpaceDelta)
{
    if (diskSpaceDelta == 0) {
        return;
    }

    auto it = DiskSpace_.find(mediumIndex);
    if (it == DiskSpace_.end()) {
        if (diskSpaceDelta != 0) {
            YT_ASSERT(diskSpaceDelta >= 0);
            DiskSpace_.emplace(mediumIndex, diskSpaceDelta);
        }
    } else {
        if (diskSpaceDelta > 0) {
            it->second.Increase(diskSpaceDelta);
        } else {
            it->second.Decrease(diskSpaceDelta);
        }
        if (it->second == TLimit64(i64(0))) {
            DiskSpace_.erase(it);
        }
    }
}

const TDefaultMap<TMediumMap<TLimit64>>& TClusterResourceLimits::DiskSpace() const
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

TClusterResourceLimits TClusterResourceLimits::Infinity()
{
    auto resources = TClusterResourceLimits()
        .SetNodeCount(TLimit64::Infinity())
        .SetTabletCount(TLimit32::Infinity())
        .SetChunkCount(TLimit64::Infinity())
        .SetTabletStaticMemory(TLimit64::Infinity())
        .SetMasterMemory(TMasterMemoryLimits::Infinity());
    resources.DiskSpace_ = TDefaultMap<TMediumMap<TLimit64>>(TLimit64::Infinity());
    return resources;
}

/*static*/ TClusterResourceLimits TClusterResourceLimits::Zero()
{
    auto resources = TClusterResourceLimits()
        .SetMasterMemory(TMasterMemoryLimits::Zero());
    return resources;
}

void TClusterResourceLimits::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, DiskSpace_.DefaultValue());
    Save(context, DiskSpace_.AsUnderlying());
    Save(context, NodeCount_);
    Save(context, ChunkCount_);
    Save(context, TabletCount_);
    Save(context, TabletStaticMemory_);
    Save(context, MasterMemory_);
}

void TClusterResourceLimits::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    // COMPAT(kvk1920)
    TLimit64 defaultDiskSpace;
    if (context.GetVersion() < EMasterReign::ReworkClusterResourceLimitsInfinityRelatedBehavior) {
        defaultDiskSpace = TLimit64(0);
    } else {
        Load(context, defaultDiskSpace);
    }
    DiskSpace_ = TDiskSpaceLimits(defaultDiskSpace);
    Load(context, DiskSpace_.AsUnderlying());
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
        auto space = Load<TLimit64>(context);
        auto mediumIndex = Load<int>(context);
        DiskSpace_[mediumIndex] = space;
    }
    Load(context, NodeCount_);
    Load(context, ChunkCount_);
    Load(context, TabletCount_);
    Load(context, TabletStaticMemory_);
    Load(context, MasterMemory_);
}

bool TClusterResourceLimits::IsViolatedBy(const TClusterResourceLimits& that) const noexcept
{
    if (this == &that) {
        return false;
    }

    for (auto [mediumIndex, thisDiskSpace] : DiskSpace()) {
        if (thisDiskSpace < that.DiskSpace().GetOrDefault(mediumIndex)) {
            return true;
        }
    }

    for (auto [mediumIndex, thatDiskSpace] : that.DiskSpace()) {
        if (DiskSpace().GetOrDefault(mediumIndex) < thatDiskSpace) {
            return true;
        }
    }

    return
        NodeCount_ < that.NodeCount_ ||
        ChunkCount_ < that.ChunkCount_ ||
        TabletCount_ < that.TabletCount_ ||
        TabletStaticMemory_ < that.TabletStaticMemory_ ||
        MasterMemory_.IsViolatedBy(that.MasterMemory());
}

TViolatedClusterResourceLimits TClusterResourceLimits::GetViolatedBy(
    const TClusterResourceLimits& that) const
{
    if (this == &that) {
        return {};
    }

    TViolatedClusterResourceLimits result;
    result.SetNodeCount(NodeCount_ < that.NodeCount_);
    result.SetChunkCount(ChunkCount_ < that.ChunkCount_);
    result.SetTabletCount(TabletCount_ < that.TabletCount_);
    result.SetTabletStaticMemory(TabletStaticMemory_ < that.TabletStaticMemory_);
    result.SetMasterMemory(MasterMemory_.GetViolatedBy(that.MasterMemory_));

    for (auto [mediumIndex, diskSpace] : DiskSpace()) {
        auto usageDiskSpace = that.DiskSpace().GetOrDefault(mediumIndex);
        if (diskSpace < usageDiskSpace) {
            result.SetMediumDiskSpace(mediumIndex, 1);
        }
    }

    for (auto [mediumIndex, usageDiskSpace] : that.DiskSpace()) {
        auto diskSpace = DiskSpace().GetOrDefault(mediumIndex);
        if (diskSpace < usageDiskSpace) {
            result.SetMediumDiskSpace(mediumIndex, 1);
        }
    }

    return result;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

template <
    bool (TLimit32::*Check32)(TLimit32) const,
    bool (TLimit64::*Check64)(TLimit64) const,
    std::optional<TViolatedMasterMemoryLimits> (TMasterMemoryLimits::*CheckMasterMemory)(const TMasterMemoryLimits&) const>
std::optional<TViolatedClusterResourceLimits> CheckChange(
    const TClusterResourceLimits& self,
    const TClusterResourceLimits& delta)
{
    std::optional<TViolatedClusterResourceLimits> result;
    auto violated = [&] () -> TViolatedClusterResourceLimits& {
        return result ? *result : result.emplace();
    };

    if (!(self.GetNodeCount().*Check64)(delta.GetNodeCount())) {
        violated().SetNodeCount(1);
    }
    if (!(self.GetChunkCount().*Check64)(delta.GetChunkCount())) {
        violated().SetChunkCount(1);
    }
    if (!(self.GetTabletCount().*Check32)(delta.GetTabletCount())) {
        violated().SetTabletCount(1);
    }
    if (!(self.GetTabletStaticMemory().*Check64)(delta.GetTabletStaticMemory())) {
        violated().SetTabletStaticMemory(1);
    }

    for (auto [mediumIndex, diskSpace] : self.DiskSpace()) {
        if (!(diskSpace.*Check64)(delta.DiskSpace().GetOrDefault(mediumIndex))) {
            violated().DiskSpace()[mediumIndex] = 1;
        }
    }

    for (auto [mediumIndex, diskSpace] : delta.DiskSpace()) {
        if (!(self.DiskSpace().GetOrDefault(mediumIndex).*Check64)(diskSpace)) {
            violated().DiskSpace()[mediumIndex] = 1;
        }
    }

    if (auto violatedMasterMemory = (self.MasterMemory().*CheckMasterMemory)(delta.MasterMemory())) {
        violated().SetMasterMemory(std::move(*violatedMasterMemory));
    }

    return result;
}

// TODO(kvk1920): Get rid of UseUnderlyingValue.
template <auto Change32, auto Change64, auto ChangeMasterMemory, bool UseUnderlyingValue = true>
void DoChange(
    TClusterResourceLimits& self,
    const TClusterResourceLimits& delta)
{
    auto doChange = [&] (auto value, auto method, auto delta) {
        auto newValue = value;
        if constexpr (UseUnderlyingValue) {
            (newValue.*method)(delta.ToUnderlying());
        } else {
            (newValue.*method)(delta);
        }
        return newValue;
    };

    self.SetNodeCount(doChange(self.GetNodeCount(), Change64, delta.GetNodeCount()));
    self.SetChunkCount(doChange(self.GetChunkCount(), Change64, delta.GetChunkCount()));
    self.SetTabletCount(doChange(self.GetTabletCount(), Change32, delta.GetTabletCount()));
    self.SetTabletStaticMemory(doChange(self.GetTabletStaticMemory(), Change64, delta.GetTabletStaticMemory()));

    (self.MasterMemory().*ChangeMasterMemory)(delta.MasterMemory());

    auto changeDiskSpace = [&] (int mediumIndex) {
        auto newDiskSpace = self.DiskSpace().GetOrDefault(mediumIndex);
        if constexpr (UseUnderlyingValue) {
            (newDiskSpace.*Change64)(delta.DiskSpace().GetOrDefault(mediumIndex).ToUnderlying());
        } else {
            (newDiskSpace.*Change64)(delta.DiskSpace().GetOrDefault(mediumIndex));
        }
        self.SetMediumDiskSpace(mediumIndex, newDiskSpace);
    };

    for (auto [mediumIndex, deltaDiskSpace] : delta.DiskSpace()) {
        changeDiskSpace(mediumIndex);
    }

    for (auto [mediumIndex, selfDiskSpace] : self.DiskSpace()) {
        if (!delta.DiskSpace().contains(mediumIndex)) {
            changeDiskSpace(mediumIndex);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::optional<TViolatedClusterResourceLimits> TClusterResourceLimits::CheckIncrease(
    const TClusterResourceLimits& delta) const
{
    return CheckChange<
        &TLimit32::CanIncrease,
        &TLimit64::CanIncrease,
        &TMasterMemoryLimits::CheckIncrease>(*this, delta);
}

std::optional<TViolatedClusterResourceLimits> TClusterResourceLimits::CheckDecrease(
    const TClusterResourceLimits& delta) const
{
    return CheckChange<
        &TLimit32::CanDecrease,
        &TLimit64::CanDecrease,
        &TMasterMemoryLimits::CheckDecrease>(*this, delta);
}

void TClusterResourceLimits::Increase(const TClusterResourceLimits& delta)
{
    DoChange<
        &TLimit32::Increase,
        &TLimit64::Increase,
        &TMasterMemoryLimits::Increase>(*this, delta);
}

void TClusterResourceLimits::Decrease(const TClusterResourceLimits& delta)
{
    DoChange<
        &TLimit32::Decrease,
        &TLimit64::Decrease,
        &TMasterMemoryLimits::Decrease>(*this, delta);
}

void TClusterResourceLimits::IncreaseWithInfinityAllowed(
    const TClusterResourceLimits& that)
{
    DoChange<
        &TLimit32::IncreaseWithInfinityAllowed,
        &TLimit64::IncreaseWithInfinityAllowed,
        &TMasterMemoryLimits::IncreaseWithInfinityAllowed,
        /*UseUnderlyingValue*/ false>(*this, that);
}

////////////////////////////////////////////////////////////////////////////////

TViolatedMasterMemoryLimits& TViolatedClusterResourceLimits::MasterMemory()
{
    return MasterMemory_;
}

const TViolatedMasterMemoryLimits& TViolatedClusterResourceLimits::MasterMemory() const
{
    return MasterMemory_;
}

void TViolatedClusterResourceLimits::SetMasterMemory(TViolatedMasterMemoryLimits masterMemoryLimits) &
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

    auto asSigned = [] (auto value) {
        return static_cast<i64>(value.ToUnderlying());
    };

    auto maybeSerialize = [&] (const char* name, auto value) {
        if (value.IsFinite()) {
            fluent.Item(name).Value(asSigned(value));
        }
    };
    maybeSerialize("node_count", resourceLimits.GetNodeCount());
    maybeSerialize("chunk_count", resourceLimits.GetChunkCount());
    maybeSerialize("tablet_count", resourceLimits.GetTabletCount());
    maybeSerialize("tablet_static_memory", resourceLimits.GetTabletStaticMemory());

    fluent
        .Item("disk_space_per_medium").DoMapFor(
            resourceLimits.DiskSpace(),
            [&] (TFluentMap fluent, auto pair) {
                auto [mediumIndex, mediumDiskSpace] = pair;
                if (mediumDiskSpace.IsInfinite()) {
                    return;
                }
                const auto* medium = chunkManager->FindMediumByIndex(mediumIndex);
                if (!IsObjectAlive(medium)) {
                    return;
                }
                fluent.Item(medium->GetName()).Value(asSigned(mediumDiskSpace));
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
                        if (!IsObjectAlive(medium) || mediumDiskSpace.IsInfinite()) {
                            // NB: It returns invalid disk space when there is
                            // an infinite limit for some medium. It's ok since
                            // in practice only root account has infinite limits.
                            return totalDiskSpace;
                        }
                        return totalDiskSpace + asSigned(mediumDiskSpace);
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
    const NCellMaster::TBootstrap* bootstrap,
    bool zeroByDefault)
{
    const auto& multicellManager = bootstrap->GetMulticellManager();
    auto result = zeroByDefault
        ? TClusterResourceLimits::Zero()
        : TClusterResourceLimits();

    auto map = node->AsMap();

    result.SetNodeCount(GetOptionalLimit64ChildOrThrow(map, "node_count"));
    result.SetChunkCount(GetOptionalLimit64ChildOrThrow(map, "chunk_count"));
    result.SetTabletCount(GetOptionalLimit32ChildOrThrow(map, "tablet_count"));
    result.SetTabletStaticMemory(GetOptionalLimit64ChildOrThrow(map, "tablet_static_memory"));

    const auto& chunkManager = bootstrap->GetChunkManager();
    if (auto diskSpacePerMediumNode = map->FindChild("disk_space_per_medium")) {
        for (const auto& [mediumName, mediumDiskSpaceNode] : diskSpacePerMediumNode->AsMap()->GetChildren()) {
            auto* medium = chunkManager->GetMediumByNameOrThrow(mediumName);
            auto mediumDiskSpace = mediumDiskSpaceNode->AsInt64()->GetValue();
            ValidateDiskSpace(mediumDiskSpace);
            result.SetMediumDiskSpace(medium->GetIndex(), TLimit64(mediumDiskSpace));
        }
    }

    if (auto masterMemoryNode = map->FindChild("master_memory")) {
        DeserializeMasterMemoryLimits(
            result.MasterMemory(),
            masterMemoryNode,
            multicellManager,
            zeroByDefault);
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
            auto fluentMap = fluent.Item("disk_space_per_medium").BeginMap();
            for (auto [mediumId, medium] : chunkManager->Media()) {
                auto mediumDiskSpace = GetOrDefault(violatedResourceLimits.DiskSpace(), medium->GetIndex(), 0);
                if (!IsObjectAlive(medium) || mediumDiskSpace == 0) {
                    continue;
                }
                fluentMap.Item(medium->GetName()).Value(mediumDiskSpace);
            }
            fluentMap.EndMap();
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
    bool serializeDiskSpace,
    std::optional<int> relevantMediumIndex)
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
        .Item("disk_space_per_medium")
        .Do([&] (auto fluent) {
            auto fluentMap = fluent.BeginMap();

            if (relevantMediumIndex) {
                auto* medium = chunkManager->FindMediumByIndex(*relevantMediumIndex);
                if (IsObjectAlive(medium)) {
                    fluentMap
                        .Item(medium->GetName())
                        .Value(GetOrDefault(violatedResourceLimits.DiskSpace(), *relevantMediumIndex, 0) != 0);
                }
            } else {
                // NB: It's important to list all media here because tablet node
                // checks medium existence by getting
                // <account>/@violated_resource_limits.
                for (auto [mediumId, medium] : chunkManager->Media()) {
                    if (!IsObjectAlive(medium)) {
                        continue;
                    }
                    fluentMap
                        .Item(medium->GetName())
                        .Value(GetOrDefault(violatedResourceLimits.DiskSpace(), medium->GetIndex(), 0) != 0);
                }
            }
            fluentMap.EndMap();
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
        if (diskSpace != TLimit64(i64(0))) {
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
