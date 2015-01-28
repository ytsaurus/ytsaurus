#pragma once

#include "public.h"

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

#ifdef YT_USE_LLVM
#include <ytlib/query_client/cg_types.h>
#endif

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
    NYPath::TYPath Path;
    NObjectClient::TObjectId TableId;

    NVersionedTableClient::TTableSchema Schema;
    NVersionedTableClient::TKeyColumns KeyColumns;
    bool Sorted = false;

    std::vector<TTabletInfoPtr> Tablets;

    TTabletInfoPtr GetTablet(NVersionedTableClient::TUnversionedRow row);

    bool NeedKeyEvaluation = false;
#ifdef YT_USE_LLVM
    std::vector<NQueryClient::TCGExpressionCallback> Evaluators;
    std::vector<NQueryClient::TCGVariables> Variables;
#endif

    void EvaluateKeys(NVersionedTableClient::TUnversionedRow fullRow, NVersionedTableClient::TRowBuffer& buffer);
    void EvaluateKeys(
        NVersionedTableClient::TUnversionedRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer,
        const NVersionedTableClient::TUnversionedRow partialRow,
        const NVersionedTableClient::TNameTableToSchemaIdMapping& idMapping);
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
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

