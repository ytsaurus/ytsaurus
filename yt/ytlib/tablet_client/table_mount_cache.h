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

#include <util/datetime/base.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TTabletInfo
    : public TRefCounted
{
    NObjectClient::TObjectId TabletId;
    i64 MountRevision = 0;
    NTabletClient::ETabletState State;
    NTableClient::TOwningKey PivotKey;
    NTabletClient::TTabletCellId CellId;
    NObjectClient::TObjectId TableId;
    TInstant UpdateTime;
    std::vector<TWeakPtr<TTableMountInfo>> Owners;
};

DEFINE_REFCOUNTED_TYPE(TTabletInfo)

////////////////////////////////////////////////////////////////////////////////

struct TTableMountInfo
    : public TRefCounted
{
    NYPath::TYPath Path;
    NObjectClient::TObjectId TableId;

    NTableClient::TTableSchema Schema;
    bool Sorted;
    bool Dynamic;
    bool NeedKeyEvaluation;

    std::vector<TTabletInfoPtr> Tablets;

    TTabletInfoPtr GetTablet(NTableClient::TUnversionedRow row) const;
    void ValidateDynamic() const;
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
    TTabletInfoPtr FindTablet(const TTabletId& tabletId);
    void InvalidateTablet(TTabletInfoPtr tabletInfo);

    void Clear();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

