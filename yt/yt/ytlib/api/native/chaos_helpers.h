#pragma once

#include "public.h"

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

namespace NYT::NApi::NNative {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBanMode,
    (None)
    (Replica)
);

struct TReplicaBanDirective
{
    EBanMode Mode = EBanMode::None;
    NChaosClient::TReplicaId ReplicaId = NObjectClient::NullObjectId;

    static TReplicaBanDirective FromError(const TError& error);
};

std::vector<NTabletClient::TTableReplicaId> GetChaosTableInSyncReplicas(
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const NChaosClient::TReplicationCardPtr& replicationCard,
    const NTableClient::TNameTablePtr& nameTable,
    const NQueryClient::TColumnEvaluatorPtr& columnEvaluator,
    const TSharedRange<NTableClient::TLegacyKey>& keys,
    bool allKeys,
    NHiveClient::TTimestamp userTimestamp);

NChaosClient::TReplicationCardPtr GetSyncReplicationCard(
    const IConnectionPtr& connection,
    NChaosClient::TReplicationCardId replicationCardId);

TTableReplicaInfoPtrList PickInSyncChaosReplicas(
    const IConnectionPtr& connection,
    const NTabletClient::TTableMountInfoPtr& tableInfo,
    const TTabletReadOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
