#pragma once

#include "public.h"

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/ytlib/object_client/public.h>

#include <yt/ytlib/table_client/chunk_meta.pb.h>
#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <yt/ytlib/ypath/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/small_vector.h>

#include <yt/core/rpc/public.h>


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
    i64 MountRevision = 0;
    NTabletClient::ETabletState State;
    NTableClient::TOwningKey PivotKey;
    NTabletClient::TTabletCellId CellId;
};

DEFINE_REFCOUNTED_TYPE(TTabletInfo)

////////////////////////////////////////////////////////////////////////////////

struct TTableMountInfo
    : public TIntrinsicRefCounted
{
    NYPath::TYPath Path;
    NObjectClient::TObjectId TableId;

    NTableClient::TTableSchema Schema;
    NTableClient::TKeyColumns KeyColumns;
    bool Sorted;
    bool Dynamic;
    bool NeedKeyEvaluation;

    std::vector<TTabletInfoPtr> Tablets;

    TTabletInfoPtr GetTablet(NTableClient::TUnversionedRow row);
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

    TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path);

    void Clear();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

