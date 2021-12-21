#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/client/chaos_client/public.h>

#include <yt/yt/core/misc/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

using NChaosClient::TReplicationCardToken;

DEFINE_ENUM(EShortcutState,
    ((Granting)     (0))
    ((Granted)      (1))
    ((Revoking)     (2))
    ((Revoked)      (3))
);

struct TCoordinatorInfo
{
    EShortcutState State;

    void Persist(const TPersistenceContext& context);
};

class TReplicationCard
    : public NTabletNode::TObjectBase
    , public TRefTracked<TReplicationCard>
{
public:
    DEFINE_BYREF_RW_PROPERTY(std::vector<NChaosClient::TReplicaInfo>, Replicas);

    using TCoordinators = THashMap<NObjectClient::TCellId, TCoordinatorInfo>;
    DEFINE_BYREF_RW_PROPERTY(TCoordinators, Coordinators);

    DEFINE_BYVAL_RW_PROPERTY(NChaosClient::TReplicationEra, Era, NChaosClient::InitialReplicationEra);

public:
    using TObjectBase::TObjectBase;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);
};

void FormatValue(TStringBuilderBase* builder, const TReplicationCard& replicationCard, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

