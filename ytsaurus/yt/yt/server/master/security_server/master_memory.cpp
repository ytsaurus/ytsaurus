#include "master_memory.h"

#include "helpers.h"

#include <yt/yt/server/master/cell_master/multicell_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NCellMaster;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TMasterMemoryLimits::TMasterMemoryLimits(
    i64 total,
    i64 chunkHost,
    THashMap<TCellTag, i64> perCell)
    : Total(total)
    , ChunkHost(chunkHost)
    , PerCell(std::move(perCell))
{ }

void TMasterMemoryLimits::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;

    Save(context, Total);
    Save(context, ChunkHost);
    Save(context, PerCell);
}

void TMasterMemoryLimits::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    Load(context, Total);
    Load(context, ChunkHost);
    Load(context, PerCell);
}

void TMasterMemoryLimits::Save(NCypressServer::TBeginCopyContext& context) const
{
    using NYT::Save;

    Save(context, Total);
    Save(context, ChunkHost);
    Save(context, PerCell);
}

void TMasterMemoryLimits::Load(NCypressServer::TEndCopyContext& context)
{
    using NYT::Load;

    Load(context, Total);
    Load(context, ChunkHost);
    Load(context, PerCell);
}

bool TMasterMemoryLimits::IsViolatedBy(const TMasterMemoryLimits& rhs) const
{
    if (this == &rhs) {
        return false;
    }

    if (Total < rhs.Total) {
        return true;
    }

    if (ChunkHost < rhs.ChunkHost) {
        return true;
    }

    for (auto [cellTag, lhsMasterMemory] : PerCell) {
        auto it = rhs.PerCell.find(cellTag);
        // No entry means there is no limit (master memory for the given cell tag is unlimited).
        if (it == rhs.PerCell.end() || lhsMasterMemory < it->second) {
            return true;
        }
    }

    return false;
}

TMasterMemoryLimits TMasterMemoryLimits::GetViolatedBy(const TMasterMemoryLimits& rhs) const
{
    if (this == &rhs) {
        return {};
    }

    TMasterMemoryLimits result;
    result.Total = Total < rhs.Total;
    result.ChunkHost = ChunkHost < rhs.ChunkHost;

    for (auto [cellTag, masterMemory] : PerCell) {
        auto it = rhs.PerCell.find(cellTag);
        if (it == rhs.PerCell.end() || masterMemory < it->second) {
            result.PerCell[cellTag] = 1;
        }
    }

    return result;
}

bool TMasterMemoryLimits::operator==(const TMasterMemoryLimits& rhs) const
{
    return
        Total == rhs.Total &&
        ChunkHost == rhs.ChunkHost &&
        PerCell == rhs.PerCell;
}

TMasterMemoryLimits& TMasterMemoryLimits::operator+=(const TMasterMemoryLimits& rhs)
{
    Total += rhs.Total;
    ChunkHost += rhs.ChunkHost;
    // No entry means there is no limit (the limit is infinite), so if either
    // lhs or rhs does not have an entry, the result should not have it as well.
    for (auto [cellTag, masterMemory] : PerCell) {
        auto it = rhs.PerCell.find(cellTag);
        if (it != rhs.PerCell.end()) {
            PerCell[cellTag] += it->second;
        } else {
            PerCell.erase(cellTag);
        }
    }

    return *this;
}

TMasterMemoryLimits& TMasterMemoryLimits::operator-=(const TMasterMemoryLimits& rhs)
{
    Total -= rhs.Total;
    ChunkHost -= rhs.ChunkHost;

    for (auto [cellTag, masterMemory] : PerCell) {
        auto it = rhs.PerCell.find(cellTag);
        if (it != rhs.PerCell.end()) {
            PerCell[cellTag] -= it->second;
        } else {
            // We are actually subtracting infinity from a finite number.
            // The result does not matter, it does not make any sense anyway.
            PerCell.erase(cellTag);
        }
    }

    // No need to iterate over rhs.PerCell: lhs and rhs intersection has already
    // been accounted for, and entries from rhs that are missing from lhs would
    // result in a infinite limit, which is equivalent to a missing entry anyway.
    return *this;
}

TMasterMemoryLimits& TMasterMemoryLimits::operator*=(i64 rhs)
{
    Total *= rhs;
    ChunkHost *= rhs;
    for (auto& [_, cellLimit] : PerCell) {
        cellLimit *= rhs;
    }
    return *this;
}

TMasterMemoryLimits TMasterMemoryLimits::operator+(const TMasterMemoryLimits& rhs) const
{
    auto result = *this;
    result += rhs;
    return result;
}

TMasterMemoryLimits TMasterMemoryLimits::operator-(const TMasterMemoryLimits& rhs) const
{
    auto result = *this;
    result -= rhs;
    return result;
}

TMasterMemoryLimits TMasterMemoryLimits::operator*(i64 rhs) const
{
    auto result = *this;
    result *= rhs;
    return result;
}

TMasterMemoryLimits TMasterMemoryLimits::operator-() const
{
    TMasterMemoryLimits result;
    result.Total = -Total;
    result.ChunkHost = -ChunkHost;
    for (auto [cellTag, cellLimit] : PerCell) {
        YT_VERIFY(result.PerCell.emplace(cellTag, cellLimit).second);
    }
    return result;
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
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("total").Value(limits.Total)
            .Item("chunk_host").Value(limits.ChunkHost)
            .Item("per_cell").DoMapFor(
                limits.PerCell,
                [&] (TFluentMap fluent, auto pair) {
                    fluent
                        .Item(multicellManager->GetMasterCellName(pair.first))
                        .Value(pair.second);
            })
        .EndMap();
}

void SerializeViolatedMasterMemoryLimits(
    const TMasterMemoryLimits& violatedLimits,
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
    const TMasterMemoryLimits& violatedLimits,
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
    const IMulticellManagerPtr& multicellManager)
{
    TMasterMemoryLimits result;

    auto map = node->AsMap();

    result.Total = GetOptionalNonNegativeI64ChildOrThrow(map, "total");
    result.ChunkHost = GetOptionalNonNegativeI64ChildOrThrow(map, "chunk_host");

    if (auto perCell = map->FindChild("per_cell")) {
        for (const auto& [cellName, cellLimitNode] : perCell->AsMap()->GetChildren()) {
            auto optionalCellTag = multicellManager->FindMasterCellTagByName(cellName);
            if (!optionalCellTag) {
                THROW_ERROR_EXCEPTION("Invalid cell name %v", cellName);
            }
            auto cellLimit = cellLimitNode->AsInt64()->GetValue();
            if (cellLimit < 0) {
                THROW_ERROR_EXCEPTION("Invalid cell master memory for %v cell: expected >= 0, found %v", cellName, cellLimit);
            }

            result.PerCell.emplace(*optionalCellTag, cellLimit);
        }
    }

    limits = std::move(result);
}

} // namespace NYT::NSecurityServer

