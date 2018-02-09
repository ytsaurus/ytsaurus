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

#include <util/datetime/base.h>

namespace NYT {
namespace NTabletClient {

////////////////////////////////////////////////////////////////////////////////

struct TTabletInfo
    : public TRefCounted
{
    NTabletClient::TTabletId TabletId;
    i64 MountRevision = 0;
    NTabletClient::ETabletState State;
    NTableClient::TOwningKey PivotKey;
    NTabletClient::TTabletCellId CellId;
    NObjectClient::TObjectId TableId;
    TInstant UpdateTime;
    std::vector<TWeakPtr<TTableMountInfo>> Owners;

    TTabletInfo& operator= (const TTabletInfo& other);
};

DEFINE_REFCOUNTED_TYPE(TTabletInfo)

////////////////////////////////////////////////////////////////////////////////

struct TTableReplicaInfo
    : public TRefCounted
{
    NTabletClient::TTableReplicaId ReplicaId;
    TString ClusterName;
    TString ReplicaPath;
    NTabletClient::ETableReplicaMode Mode;
};

DEFINE_REFCOUNTED_TYPE(TTableReplicaInfo)

////////////////////////////////////////////////////////////////////////////////

//! Describes the primary and the auxiliary schemas derived from the table schema.
//! Cf. TTableSchema::ToXXX methods.
DEFINE_ENUM(ETableSchemaKind,
    // Schema assigned to Cypress node, as is.
    (Primary)
    // Schema used for inserting rows.
    (Write)
    // Schema used for querying rows.
    (Query)
    // Schema used for deleting rows.
    (Delete)
    // Schema used for writing versioned rows (during replication).
    (VersionedWrite)
    // Schema used for looking up rows.
    (Lookup)
);

struct TTableMountInfo
    : public TRefCounted
{
    NYPath::TYPath Path;
    NObjectClient::TObjectId TableId;
    TEnumIndexedVector<NTableClient::TTableSchema, ETableSchemaKind> Schemas;

    bool Dynamic;
    NTabletClient::TTableReplicaId UpstreamReplicaId;
    bool NeedKeyEvaluation;

    std::vector<TTabletInfoPtr> Tablets;
    std::vector<TTabletInfoPtr> MountedTablets;

    std::vector<TTableReplicaInfoPtr> Replicas;

    //! For sorted tables, these are -infinity and +infinity.
    //! For ordered tablets, these are |[0]| and |[tablet_count]| resp.
    NTableClient::TOwningKey LowerCapBound;
    NTableClient::TOwningKey UpperCapBound;

    bool IsSorted() const;
    bool IsOrdered() const;
    bool IsReplicated() const;

    TTabletInfoPtr GetTabletByIndexOrThrow(int tabletIndex) const;
    TTabletInfoPtr GetTabletForRow(const TRange<NTableClient::TUnversionedValue>& row) const;
    TTabletInfoPtr GetTabletForRow(NTableClient::TUnversionedRow row) const;
    TTabletInfoPtr GetTabletForRow(NTableClient::TVersionedRow row) const;
    TTabletInfoPtr GetRandomMountedTablet() const;

    void ValidateDynamic() const;
    void ValidateSorted() const;
    void ValidateOrdered() const;
    void ValidateNotReplicated() const;
    void ValidateReplicated() const;
};

DEFINE_REFCOUNTED_TYPE(TTableMountInfo)

////////////////////////////////////////////////////////////////////////////////

struct ITableMountCache
    : public virtual TRefCounted
{
    virtual TFuture<TTableMountInfoPtr> GetTableInfo(const NYPath::TYPath& path) = 0;
    virtual TTabletInfoPtr FindTablet(const TTabletId& tabletId) = 0;
    virtual void InvalidateTablet(TTabletInfoPtr tabletInfo) = 0;
    virtual std::pair<bool, TTabletInfoPtr> InvalidateOnError(const TError& error) = 0;

    virtual void Clear() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableMountCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletClient
} // namespace NYT

