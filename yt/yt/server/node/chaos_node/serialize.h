#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/serialize.h>

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
    ((SaneTxActionAbort)            (300013)) // kvk1920
    ((SaneTxActionAbortFix)         (300014)) // kvk1920
    ((ClockClusterTagValidation)    (300015)) // osidorkin
    ((CollocationOptions)           (300016)) // akozhikhov
    ((AttachDistributedCollocation) (300017)) // savrus
    ((NoDetachOnDistributedCollocationAttach)   (300018)) // osidorkin
);

static_assert(TEnumTraits<EChaosReign>::IsMonotonic, "Chaos reign enum is not monotonic");

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    TSaveContext(
        NHydra::ICheckpointableOutputStream* output,
        NLogging::TLogger logger);

    EChaosReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    using NHydra::TLoadContext::TLoadContext;

    EChaosReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode

#define SERIALIZE_INL_H_
#include "serialize-inl.h"
#undef SERIALIZE_INL_H_
