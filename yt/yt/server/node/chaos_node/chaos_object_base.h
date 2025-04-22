#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/client/chaos_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

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

struct TMigration
{
    NObjectClient::TCellId OriginCellId;
    NObjectClient::TCellId ImmigratedToCellId;
    NObjectClient::TCellId EmigratedFromCellId;
    TInstant ImmigrationTime;
    TInstant EmigrationTime;

    void Persist(const TPersistenceContext& context);
};

class TChaosObjectBase
    : public NTabletNode::TObjectBase
{
public:
    using TCoordinators = THashMap<NObjectClient::TCellId, TCoordinatorInfo>;
    DEFINE_BYREF_RW_PROPERTY(TCoordinators, Coordinators);

    DEFINE_BYVAL_RW_PROPERTY(NChaosClient::TReplicationEra, Era, NChaosClient::InitialReplicationEra);

    DEFINE_BYREF_RW_PROPERTY(TMigration, Migration);
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TObjectId, MigrationToken);

public:
    using NTabletNode::TObjectBase::TObjectBase;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
