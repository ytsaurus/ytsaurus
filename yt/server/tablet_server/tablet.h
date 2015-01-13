#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/enum.h>
#include <core/misc/ref_tracked.h>
#include <core/misc/nullable.h>

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/node_tracker_client/node_tracker_service.pb.h>

#include <server/object_server/object.h>

#include <server/table_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct TTabletStatistics
{
    TNullable<int> PartitionCount = 0;
    TNullable<int> StoreCount = 0;
    i64 UnmergedRowCount = 0;
    i64 UncompressedDataSize = 0;
    i64 CompressedDataSize = 0;
    i64 DiskSpace = 0;
    int ChunkCount = 0;
};

void Serialize(const TTabletStatistics& statistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TTablet
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TTablet>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, Index);
    DEFINE_BYVAL_RW_PROPERTY(ETabletState, State);
    DEFINE_BYVAL_RW_PROPERTY(NTableServer::TTableNode*, Table);
    DEFINE_BYVAL_RW_PROPERTY(TTabletCell*, Cell);
    DEFINE_BYVAL_RW_PROPERTY(NVersionedTableClient::TOwningKey, PivotKey);
    DEFINE_BYREF_RW_PROPERTY(NNodeTrackerClient::NProto::TTabletStatistics, NodeStatistics);

public:
    explicit TTablet(const TTabletId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
