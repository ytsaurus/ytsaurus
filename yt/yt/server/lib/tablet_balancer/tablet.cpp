#include "tablet.h"

#include <yt/yt/core/ytree/node.h>

namespace NYT::NTabletBalancer {

////////////////////////////////////////////////////////////////////////////////

void Deserialize(TTabletStatistics& value, NYTree::INodePtr node)
{
    auto mapNode = node->AsMap();

    value.CompressedDataSize = mapNode->FindChild("compressed_data_size")->AsInt64()->GetValue();
    value.UncompressedDataSize = mapNode->FindChild("uncompressed_data_size")->AsInt64()->GetValue();
    value.MemorySize = mapNode->FindChild("memory_size")->AsInt64()->GetValue();
    value.PartitionCount = mapNode->FindChild("partition_count")->AsInt64()->GetValue();
}

////////////////////////////////////////////////////////////////////////////////

TTablet::TTablet(
    TTabletId tabletId,
    TTable* table)
    : Id(tabletId)
    , Table(std::move(table))
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletBalancer
