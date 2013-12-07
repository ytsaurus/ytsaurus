#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/future.h>

#include <core/rpc/public.h>

#include <ytlib/ypath/public.h>

#include <ytlib/object_client/public.h>

#include <ytlib/hive/public.h>

#include <ytlib/tablet_client/public.h>

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/schema.h>
#include <ytlib/new_table_client/row.h>
#include <ytlib/new_table_client/chunk_meta.pb.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct TTabletInfo
{
    NObjectClient::TObjectId TabletId;
    NTabletClient::ETabletState State;
    NVersionedTableClient::TOwningKey PivotKey;
    NTabletClient::TTabletCellId CellId;
};

struct TTableMountInfo
    : public TIntrinsicRefCounted
{
    NVersionedTableClient::TTableSchema Schema;
    NVersionedTableClient::TKeyColumns KeyColumns;
    NObjectClient::TObjectId TableId;
    std::vector<TTabletInfo> Tablets;
};

class TTableMountCache
    : public TRefCounted
{
public:
    TTableMountCache(
        TTableMountCacheConfigPtr config,
        NRpc::IChannelPtr masterChannel,
        NHive::TCellDirectoryPtr cellDirectory);

    ~TTableMountCache();

    TFuture<TErrorOr<TTableMountInfoPtr>> LookupInfo(const NYPath::TYPath& path);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

