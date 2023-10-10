#include "holders.h"

#include <yt/yt/server/lib/tablet_balancer/config.h>
#include <yt/yt/server/lib/tablet_balancer/table.h>
#include <yt/yt/server/lib/tablet_balancer/tablet.h>
#include <yt/yt/server/lib/tablet_balancer/tablet_cell.h>

namespace NYT::NTabletBalancer::NDryRun {

using namespace NObjectClient;
using namespace NTabletBalancer;
using namespace NTabletClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static const TString UncompressedDataSizeField = "uncompressed_data_size";
static const TString CompressedDataSizeField = "compressed_data_size";
static const TString MemorySizeField = "memory_size";
static const TString PartitionCountField = "partition_count";

////////////////////////////////////////////////////////////////////////////////

TTabletPtr TTabletHolder::CreateTablet(
    const TTablePtr& table,
    const THashMap<TTabletCellId, TTabletCellPtr>& cells) const
{
    auto tablet = New<TTablet>(
        TabletId,
        table.Get());

    tablet->Statistics.UncompressedDataSize = GetOrCrash(Statistics, UncompressedDataSizeField);
    tablet->Statistics.CompressedDataSize = GetOrCrash(Statistics, CompressedDataSizeField);
    tablet->Statistics.MemorySize = GetOrCrash(Statistics, MemorySizeField);
    tablet->Statistics.PartitionCount = GetOrCrash(Statistics, PartitionCountField);
    tablet->Statistics.OriginalNode = ConvertTo<INodePtr>(Statistics);
    tablet->PerformanceCounters = TYsonString(PerformanceCounters);

    if (auto cellIt = cells.find(CellId); cellIt != cells.end()) {
        tablet->Cell = cellIt->second;
        tablet->State = ETabletState::Mounted;
        EmplaceOrCrash(cellIt->second->Tablets, TabletId, tablet);

        tablet->Index = std::ssize(table->Tablets);
        table->Tablets.push_back(tablet);
    }

    return tablet;
}

void TTabletHolder::Register(TRegistrar registrar)
{
    registrar.Parameter("statistics", &TThis::Statistics)
        .Default();
    registrar.Parameter("performance_counters", &TThis::PerformanceCounters)
        .Default();
    registrar.Parameter("tablet_id", &TThis::TabletId)
        .Default();
    registrar.Parameter("cell_id", &TThis::CellId)
        .Default();
    registrar.Parameter("tablet_index", &TThis::TabletIndex)
        .Default(0);
    registrar.Parameter("cell_index", &TThis::CellIndex)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

TTablePtr TTableHolder::CreateTable(const TTabletCellBundlePtr& bundle) const
{
    auto table = New<TTable>(
        /*sorted*/ true,
        ToString(TableId),
        MinValidCellTag,
        TableId,
        bundle.Get());

    table->Dynamic = true;
    table->CompressedDataSize = CompressedDataSize;
    table->UncompressedDataSize = UncompressedDataSize;
    table->InMemoryMode = InMemoryMode;
    table->TableConfig = Config;
    table->TableConfig->EnableVerboseLogging = true;
    return table;
}

void TTableHolder::Register(TRegistrar registrar)
{
    registrar.Parameter("compressed_data_size", &TThis::CompressedDataSize)
        .Default(0);
    registrar.Parameter("uncompressed_data_size", &TThis::UncompressedDataSize)
        .Default(0);
    registrar.Parameter("in_memory_mode", &TThis::InMemoryMode)
        .Default(EInMemoryMode::None);
    registrar.Parameter("tablets", &TThis::Tablets)
        .Default();
    registrar.Parameter("config", &TThis::Config)
        .DefaultNew();
    registrar.Parameter("table_id", &TThis::TableId)
        .Default();
    registrar.Parameter("table_index", &TThis::TableIndex)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundle::TNodeStatistics TNodeHolder::GetStatistics() const
{
    TTabletCellBundle::TNodeStatistics statistics{
        .TabletSlotCount = TabletSlotCount,
        .MemoryUsed = MemoryUsed
    };
    if (MemoryLimit) {
        statistics.MemoryLimit = *MemoryLimit;
    }
    return statistics;
}

void TNodeHolder::Register(TRegistrar registrar)
{
    registrar.Parameter("node_address", &TThis::NodeAddress)
        .Default();
    registrar.Parameter("tablet_slot_count", &TThis::TabletSlotCount)
        .Default(1)
        .GreaterThanOrEqual(0);
    registrar.Parameter("memory_used", &TThis::MemoryUsed)
        .Default(0);
    registrar.Parameter("memory_limit", &TThis::MemoryLimit)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TTabletCellPtr TTabletCellHolder::CreateCell() const
{
    return New<TTabletCell>(
        CellId,
        TTabletCellStatistics{.MemorySize = MemorySize},
        TTabletCellStatus{.Health = ETabletCellHealth::Good, .Decommissioned = false},
        NodeAddress,
        ETabletCellLifeStage::Running);
}

void TTabletCellHolder::Register(TRegistrar registrar)
{
    registrar.Parameter("memory_size", &TThis::MemorySize)
        .Default(0);
    registrar.Parameter("node_address", &TThis::NodeAddress)
        .Default();
    registrar.Parameter("cell_id", &TThis::CellId)
        .Default();
    registrar.Parameter("cell_index", &TThis::CellIndex)
        .Default(0);
}

////////////////////////////////////////////////////////////////////////////////

TTabletCellBundlePtr TBundleHolder::CreateBundle() const
{
    auto bundle = New<TTabletCellBundle>("bundle");
    bundle->Config = Config;
    bundle->Config->EnableVerboseLogging = true;

    for (const auto& nodeHolder : Nodes) {
        EmplaceOrCrash(
            bundle->NodeStatistics,
            nodeHolder->NodeAddress,
            nodeHolder->GetStatistics());
    }

    for (const auto& cellHolder : Cells) {
        auto cell = cellHolder->CreateCell();
        EmplaceOrCrash(bundle->TabletCells, cellHolder->CellId, std::move(cell));
    }

    for (const auto& tableHolder : Tables) {
        auto table = tableHolder->CreateTable(bundle);
        EmplaceOrCrash(bundle->Tables, table->Id, table);
        for (const auto& tabletHolder : tableHolder->Tablets) {
            tabletHolder->CreateTablet(table, bundle->TabletCells);
        }
    }

    return bundle;
}

void TBundleHolder::Register(TRegistrar registrar)
{
    registrar.Parameter("config", &TThis::Config)
        .DefaultNew();
    registrar.Parameter("tables", &TThis::Tables)
        .Default();
    registrar.Parameter("nodes", &TThis::Nodes)
        .Default();
    registrar.Parameter("cells", &TThis::Cells)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer::NDryRun
