#pragma once

#include "connection.h"
#include "private.h"

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

class TTableReplicaSynchronicityCache
    : public TRefCounted
{
public:
    TTableReplicaSynchronicityCache() = default;

    TFuture<TReplicaSynchronicityList> GetReplicaSynchronicities(
        const IConnectionPtr& connection,
        const NTabletClient::TTableMountInfoPtr& table,
        TInstant deadline,
        const TTabletReadOptions& options);

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, Lock_);
    THashMap<NYPath::TYPath, TTimestampedReplicaSynchronicities> TableToReplicaSynchronicities_;

    TReplicaSynchronicityList OnReplicaSynchronicitiesFetched(
        NYPath::TYPath path,
        TReplicaSynchronicityList replicas);
};

DEFINE_REFCOUNTED_TYPE(TTableReplicaSynchronicityCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
