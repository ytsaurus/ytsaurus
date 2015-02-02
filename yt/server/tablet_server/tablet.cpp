#include "stdafx.h"
#include "tablet.h"
#include "tablet_cell.h"

#include <core/ytree/fluent.h>

#include <server/table_server/table_node.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NTabletServer {

using namespace NTableServer;
using namespace NCellMaster;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTabletStatistics& statistics, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(static_cast<bool>(statistics.PartitionCount), [&] (TFluentMap fluent) {
                fluent.Item("partition_count").Value(*statistics.PartitionCount);
            })
            .DoIf(static_cast<bool>(statistics.StoreCount), [&] (TFluentMap fluent) {
                fluent.Item("store_count").Value(*statistics.StoreCount);
            })
            .Item("unmerged_row_count").Value(statistics.UnmergedRowCount)
            .Item("uncompressed_data_size").Value(statistics.UncompressedDataSize)
            .Item("compressed_data_size").Value(statistics.CompressedDataSize)
            .Item("disk_space").Value(statistics.DiskSpace)
            .Item("chunk_count").Value(statistics.ChunkCount)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(const TTabletId& id)
    : TNonversionedObjectBase(id)
    , Index_(-1)
    , State_(ETabletState::Unmounted)
    , Table_(nullptr)
    , Cell_(nullptr)
{ }

void TTablet::Save(TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Index_);
    Save(context, State_);
    Save(context, Table_);
    Save(context, Cell_);
    Save(context, PivotKey_);
    Save(context, NodeStatistics_);
}

void TTablet::Load(TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Index_);
    Load(context, State_);
    Load(context, Table_);
    Load(context, Cell_);
    Load(context, PivotKey_);
    // COMPAT(babenko)
    if (context.GetVersion() >= 111) {
        Load(context, NodeStatistics_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT

