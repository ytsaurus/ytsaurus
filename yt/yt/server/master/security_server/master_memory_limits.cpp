#include "master_memory_limits.h"

#include "helpers.h"

#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TMasterMemoryLimits::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Total);
    Save(context, ChunkHost);
    Save(context, PerCell.DefaultValue());
    Save(context, PerCell.AsUnderlying());
}

void TMasterMemoryLimits::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, Total);
    Load(context, ChunkHost);

    TLimit64 defaultPerCellLimit;
    // COMPAT(kvk1920)
    if (context.GetVersion() < EMasterReign::ReworkClusterResourceLimitsInfinityRelatedBehavior) {
        defaultPerCellLimit = TLimit64::Infinity();
    } else {
        Load(context, defaultPerCellLimit);
    }
    PerCell = TPerCellLimits(defaultPerCellLimit);
    Load(context, PerCell.AsUnderlying());
}

void TMasterMemoryLimits::Save(NCypressServer::TBeginCopyContext& context) const
{
    using NYT::Save;

    Save(context, Total);
    Save(context, ChunkHost);
    Save(context, PerCell.DefaultValue());
    Save(context, PerCell.AsUnderlying());
}

void TMasterMemoryLimits::Load(NCypressServer::TEndCopyContext& context)
{
    using NYT::Load;

    Load(context, Total);
    Load(context, ChunkHost);
    PerCell = TPerCellLimits(Load<TLimit64>(context));
    Load(context, PerCell.AsUnderlying());
}

bool TMasterMemoryLimits::IsViolatedBy(const TMasterMemoryLimits& that) const noexcept
{
    if (this == &that) {
        return false;
    }

    if (Total < that.Total) {
        return true;
    }

    if (ChunkHost < that.ChunkHost) {
        return true;
    }

    for (auto [cellTag, masterMemory] : PerCell) {
        if (that.PerCell.GetOrDefault(cellTag) > masterMemory) {
            return true;
        }
    }

    return false;
}

TViolatedMasterMemoryLimits TMasterMemoryLimits::GetViolatedBy(const TMasterMemoryLimits& that) const
{
    TViolatedMasterMemoryLimits result = {
        .Total = that.Total > Total,
        .ChunkHost = that.ChunkHost > ChunkHost,
    };

    for (auto [cellTag, masterMemory] : PerCell) {
        if (that.PerCell.GetOrDefault(cellTag) > masterMemory) {
            result.PerCell[cellTag] = 1;
        }
    }

    return result;
}

namespace {

////////////////////////////////////////////////////////////////////////////////

template <bool (TLimit64::*Method)(TLimit64) const noexcept>
std::optional<TViolatedMasterMemoryLimits> CheckChange(
    const TMasterMemoryLimits& self,
    const TMasterMemoryLimits& delta) noexcept
{
    std::optional<TViolatedMasterMemoryLimits> result;
    auto violated = [&] () {
        return result ? &*result : &result.emplace();
    };

    if (!(self.Total.*Method)(delta.Total)) {
        violated()->Total = 1;
    }

    if (!(self.ChunkHost.*Method)(delta.ChunkHost)) {
        violated()->ChunkHost = 1;
    }

    for (auto [cellTag, thisLimit] : self.PerCell) {
        if (!(thisLimit.*Method)(delta.PerCell.GetOrDefault(cellTag))) {
            violated()->PerCell[cellTag] = 1;
        }
    }

    for (auto [cellTag, limitDelta] : delta.PerCell) {
        if (!(self.PerCell.GetOrDefault(cellTag).*Method)(limitDelta)) {
            violated()->PerCell[cellTag] = 1;
        }
    }

    return result;
}

template <auto method, bool UseUnderlying = true>
void DoChange(
    TMasterMemoryLimits& self,
    const TMasterMemoryLimits& delta)
{
    if constexpr (UseUnderlying) {
        (self.Total.*method)(delta.Total.ToUnderlying());
        (self.ChunkHost.*method)(delta.ChunkHost.ToUnderlying());
    } else {
        (self.Total.*method)(delta.Total);
        (self.ChunkHost.*method)(delta.ChunkHost);
    }

    TCellTagList defaultValues;

    for (auto [cellTag, deltaMasterMemory] : delta.PerCell) {
        if (deltaMasterMemory == TLimit64(i64(0))) {
            continue;
        }

        auto& thisLimit = self.PerCell[cellTag];
        if constexpr (UseUnderlying) {
            (thisLimit.*method)(deltaMasterMemory.ToUnderlying());
        } else {
            (thisLimit.*method)(deltaMasterMemory);
        }
        if (thisLimit == self.PerCell.DefaultValue()) {
            defaultValues.push_back(cellTag);
        }
    }

    for (auto cellTag : defaultValues) {
        self.PerCell.erase(cellTag);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

std::optional<TViolatedMasterMemoryLimits> TMasterMemoryLimits::CheckIncrease(
    const TMasterMemoryLimits& delta) const
{
    return CheckChange<&TLimit64::CanIncrease>(*this, delta);
}

std::optional<TViolatedMasterMemoryLimits> TMasterMemoryLimits::CheckDecrease(
    const TMasterMemoryLimits& delta) const
{
    return CheckChange<&TLimit64::CanDecrease>(*this, delta);
}

void TMasterMemoryLimits::Increase(const TMasterMemoryLimits& delta)
{
    DoChange<&TLimit64::Increase>(*this, delta);
}

void TMasterMemoryLimits::IncreaseWithInfinityAllowed(const TMasterMemoryLimits& that)
{
    DoChange<&TLimit64::IncreaseWithInfinityAllowed, /*UseUnderlying*/ false>(*this, that);
}

void TMasterMemoryLimits::Decrease(const TMasterMemoryLimits& delta)
{
    DoChange<&TLimit64::Decrease>(*this, delta);
}

TMasterMemoryLimits TMasterMemoryLimits::Zero()
{
    return TMasterMemoryLimits{
        .Total{i64(0)},
        .ChunkHost{i64(0)},
        .PerCell{TLimit64(i64(0))},
    };
}

TMasterMemoryLimits TMasterMemoryLimits::Infinity()
{
    return TMasterMemoryLimits{
        .Total = TLimit64::Infinity(),
        .ChunkHost = TLimit64::Infinity(),
        .PerCell{TLimit64::Infinity()},
    };
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TMasterMemoryLimits& limits, TStringBuf /*format*/)
{
    builder->AppendFormat(
        "{Total: %v, ChunkHost: %v, PerCell: %v}",
        limits.Total,
        limits.ChunkHost,
        limits.PerCell);
}

TString ToString(const TMasterMemoryLimits& limits)
{
    return ToStringViaBuilder(limits);
}

////////////////////////////////////////////////////////////////////////////////

void SerializeMasterMemoryLimits(
    const TMasterMemoryLimits& limits,
    NYson::IYsonConsumer* consumer,
    const IMulticellManagerPtr& multicellManager)
{
    auto asSigned = [] (auto value) {
        return static_cast<i64>(value.ToUnderlying());
    };

    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(!limits.Total.IsInfinite(), [&] (auto map) {
                map.Item("total").Value(asSigned(limits.Total));
            })
            .DoIf(!limits.ChunkHost.IsInfinite(), [&] (auto map) {
                map.Item("chunk_host").Value(asSigned(limits.ChunkHost));
            })
            .Item("per_cell").DoMapFor(
                limits.PerCell,
                [&] (TFluentMap fluent, auto pair) {
                    if (!pair.second.IsInfinite()) {
                        fluent
                            .Item(multicellManager->GetMasterCellName(pair.first))
                            .Value(asSigned(pair.second));
                    }
                })
        .EndMap();
}

void SerializeViolatedMasterMemoryLimits(
    const TViolatedMasterMemoryLimits& violatedLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::IMulticellManagerPtr& multicellManager)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(violatedLimits.Total)
            .Item("chunk_host").Value(violatedLimits.ChunkHost)
            .Item("per_cell").DoMapFor(
                violatedLimits.PerCell,
                [&] (TFluentMap fluent, auto pair) {
                    fluent
                        .Item(multicellManager->GetMasterCellName(pair.first))
                        .Value(pair.second);
            })
        .EndMap();
}

void SerializeViolatedMasterMemoryLimitsInBooleanFormat(
    const TViolatedMasterMemoryLimits& violatedLimits,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::IMulticellManagerPtr& multicellManager)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(violatedLimits.Total != 0)
            .Item("chunk_host").Value(violatedLimits.ChunkHost != 0)
            .Item("per_cell").DoMapFor(
                violatedLimits.PerCell,
                [&] (TFluentMap fluent, auto pair) {
                    fluent
                        .Item(multicellManager->GetMasterCellName(pair.first))
                        .Value(pair.second != 0);
            })
        .EndMap();
}

void DeserializeMasterMemoryLimits(
    TMasterMemoryLimits& limits,
    NYTree::INodePtr node,
    const IMulticellManagerPtr& multicellManager,
    bool zeroByDefault)
{
    auto result = zeroByDefault
        ? TMasterMemoryLimits::Zero()
        : TMasterMemoryLimits{};

    auto map = node->AsMap();

    result.Total = GetOptionalLimit64ChildOrThrow(map, "total");
    result.ChunkHost = GetOptionalLimit64ChildOrThrow(map, "chunk_host");

    if (auto perCell = map->FindChild("per_cell")) {
        for (const auto& [cellName, cellLimitNode] : perCell->AsMap()->GetChildren()) {
            auto optionalCellTag = multicellManager->FindMasterCellTagByName(cellName);
            if (!optionalCellTag) {
                THROW_ERROR_EXCEPTION("Invalid cell name %v", cellName);
            }
            auto cellLimit = cellLimitNode->AsInt64()->GetValue();
            if (cellLimit < 0) {
                THROW_ERROR_EXCEPTION(
                    "Invalid cell master memory for %v cell: expected >= 0, found %v",
                    cellName,
                    cellLimit);
            }

            result.PerCell[*optionalCellTag] = TLimit64(cellLimit);
        }
    }

    limits = std::move(result);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
