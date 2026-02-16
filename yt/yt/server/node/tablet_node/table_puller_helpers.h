#pragma once

#include "public.h"

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/logging/logger.h>

#include <library/cpp/yt/error/error.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TBannedReplicaTracker
{
public:
    struct TBanInfo
    {
        int Counter;
        TError LastError;
    };

    TBannedReplicaTracker(NLogging::TLogger logger, std::optional<int> replicaBanDuration);

    bool IsReplicaBanned(NChaosClient::TReplicaId replicaId) const;
    const THashMap<NChaosClient::TReplicaId, TBanInfo>& GetBannedReplicas() const;
    void BanReplica(NChaosClient::TReplicaId replicaId, TError error);
    void SyncReplicas(const NChaosClient::TReplicationCardPtr& replicationCard);

private:
    const NLogging::TLogger Logger;
    const std::optional<int> ReplicaBanDuration_;

    THashMap<NChaosClient::TReplicaId, TBanInfo> BannedReplicas_;

    void DecreaseCounters();
};

////////////////////////////////////////////////////////////////////////////////

class TQueueReplicaSelector
{
public:
    using TReplicaOrError = TErrorOr<std::tuple<
        NChaosClient::TReplicaId,
        NChaosClient::TReplicaInfo*,
        NHiveClient::TTimestamp>>;

public:
    TQueueReplicaSelector(
        NLogging::TLogger logger,
        const TBannedReplicaTracker& bannedReplicaTracker);

    TReplicaOrError PickQueueReplica(
        NChaosClient::TReplicaId selfUpstreamReplicaId,
        const NChaosClient::TReplicationCardPtr& replicationCard,
        const NChaosClient::TReplicationProgress& replicationProgress,
        TInstant now);

    void ResetLastPulledFromReplicaId();

private:
    const NLogging::TLogger Logger;
    const TBannedReplicaTracker& BannedReplicaTracker_;

    NChaosClient::TReplicaId LastPulledFromReplicaId_;
    TInstant NextPermittedTimeForProgressBehindAlert_;
};

////////////////////////////////////////////////////////////////////////////////

}
