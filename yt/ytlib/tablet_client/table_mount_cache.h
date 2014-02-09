#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/small_vector.h>

#include <core/actions/future.h>

#include <core/rpc/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/hive/public.h>

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TTabletReplica
{
    TTabletReplica();
    TTabletReplica(
        NNodeTrackerClient::TNodeId id,
        const NNodeTrackerClient::TNodeDescriptor& descriptor);

    NNodeTrackerClient::TNodeId Id;
    NNodeTrackerClient::TNodeDescriptor Descriptor;
};

////////////////////////////////////////////////////////////////////////////////

struct TTabletInfo
    : public TIntrinsicRefCounted
{
    NObjectClient::TObjectId TabletId;
    NTabletClient::ETabletState State;
    NVersionedTableClient::TOwningKey PivotKey;
    NTabletClient::TTabletCellId CellId;
    SmallVector<TTabletReplica, TypicalCellSize> Replicas;
};

DEFINE_REFCOUNTED_TYPE(TTabletInfo)

////////////////////////////////////////////////////////////////////////////////

struct TTableMountInfo
    : public TIntrinsicRefCounted
{
    TTableMountInfo();

    NVersionedTableClient::TTableSchema Schema;
    NVersionedTableClient::TKeyColumns KeyColumns;
    NObjectClient::TObjectId TableId;
    std::vector<TTabletInfoPtr> Tablets;
    bool Sorted;

    TTabletInfoPtr GetTablet(NVersionedTableClient::TUnversionedRow row);

};

DEFINE_REFCOUNTED_TYPE(TTableMountInfo)

////////////////////////////////////////////////////////////////////////////////

class TTableMountCache
    : public TRefCounted
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NHive::TCellDirectoryPtr cellDirectory);
    ~TTableMountCache();

    TFuture<TErrorOr<TTableMountInfoPtr>> LookupTableInfo(const NYPath::TYPath& path);

    TTabletInfoPtr FindTabletInfo(const TTabletId& id);
    TTabletInfoPtr GetTabletInfoOrThrow(const TTabletId& id);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

