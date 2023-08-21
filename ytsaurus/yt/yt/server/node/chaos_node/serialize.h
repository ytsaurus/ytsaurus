#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/serialize.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EChaosReign,
    ((LetTheChaosBegin)             (300001)) // savrus
    ((CurrentTimestamp)             (300002)) // savrus
    ((RemoveCommitted)              (300003)) // savrus
    ((Migration)                    (300004)) // savrus
    ((ReplicatedTableOptions)       (300005)) // savrus
    ((SupportQueueReplicasInRTT)    (300006)) // akozhikhov
    ((ReplicationCardCollocation)   (300007)) // savrus
    ((AllowAlterInCataclysm)        (300008)) // savrus
    ((ChaosCellSuspension)          (300009)) // savrus
    ((RevokeFromSuspended)          (300010)) // savrus
    ((RemoveMigratedCards)          (300011)) // ponasenko-rs
    ((ConfirmMigrations)            (300012)) // ponasenko-rs
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    explicit TSaveContext(NHydra::ICheckpointableOutputStream* output);

    EChaosReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    explicit TLoadContext(NHydra::ICheckpointableInputStream* input);

    EChaosReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
