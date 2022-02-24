#pragma once

#include "public.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TExpiredReplicaHistory;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TExpiredReplicaHistory
{
    NChaosClient::TReplicaId ReplicaId;
    NTransactionClient::TTimestamp RetainTimestamp;
};

void ToProto(NProto::TExpiredReplicaHistory* protoHistoryForsake, const TExpiredReplicaHistory& historyForsake);
void FromProto(TExpiredReplicaHistory* historyForsake, const NProto::TExpiredReplicaHistory& protoHistoryForsake);

////////////////////////////////////////////////////////////////////////////////

struct IReplicationCardObserver
    : public TRefCounted
{
    virtual void Start() = 0;
    virtual void Stop() = 0;
};

DEFINE_REFCOUNTED_TYPE(IReplicationCardObserver)

IReplicationCardObserverPtr CreateReplicationCardObserver(
    TReplicationCardObserverConfigPtr config,
    IChaosSlotPtr slot);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
