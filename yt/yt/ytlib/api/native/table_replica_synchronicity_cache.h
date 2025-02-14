#pragma once

#include "connection.h"
#include "private.h"
#include "public.h"

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

struct TReplicaSynchronicity
{
    NTabletClient::TTableReplicaInfoPtr ReplicaInfo;
    NTableClient::TTimestamp MinReplicationTimestamp = NTransactionClient::NullTimestamp;
    bool IsInSync = false;
};

struct TTimestampedReplicaSynchronicities
{
    TInstant CachedAt;
    TReplicaSynchronicityList ReplicaSynchronicities;
};

void FormatValue(TStringBuilderBase* builder, const TReplicaSynchronicity& replica, TStringBuf spec = {});

////////////////////////////////////////////////////////////////////////////////

TFuture<TReplicaSynchronicityList> FetchReplicaSynchronicities(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableMountInfo,
    const TTabletReadOptions& options);

////////////////////////////////////////////////////////////////////////////////

struct ITableReplicaSynchronicityCache
    : public TRefCounted
{
    virtual TFuture<TReplicaSynchronicityList> GetReplicaSynchronicities(
        const IConnectionPtr& connection,
        const NTabletClient::TTableMountInfoPtr& table,
        TInstant deadline,
        const TTabletReadOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITableReplicaSynchronicityCache)

////////////////////////////////////////////////////////////////////////////////

ITableReplicaSynchronicityCachePtr CreateTableReplicaSynchronicityCache();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
