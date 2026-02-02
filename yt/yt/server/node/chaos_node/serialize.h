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
    ((ClockClusterTagValidation)                (300015)) // osidorkin
    ((CollocationOptions)                       (300016)) // akozhikhov
    ((AttachDistributedCollocation)             (300017)) // savrus
    ((NoDetachOnDistributedCollocationAttach)   (300018)) // osidorkin
    ((ForbidSyncQueuesCountBelowLimit)          (300019)) // osidorkin
    // 25.2 starts here
    ((PersistMigrationToken)                    (300101)) // gryzlov-ad
    ((IntroduceChaosObjectAndLease)             (300102)) // gryzlov-ad
    ((FixTransactionActionAbort)                (300103)) // kvk1920
    ((TransactionActionStates)                  (300104)) // babenko
    ((PersistTransactionSignature_25_2)         (300105)) // ponasenko-rs
    // 25.3 starts here
    ((AddLastCoordinatorCommitTimestamp)        (300201)) // aleksandra-zh
    ((PersistTransactionSignature)              (300202)) // ponasenko-rs
    ((DoNotSkipCommenceNewEraIfNoReplicas)      (300203)) // gryzlov-ad
    // 25.4 starts here
    ((Start_25_4)                               (300300)) // h0pless
    ((HydraLogicalRecordId)                     (300301)) // h0pless
    ((IntroduceChaosLeaseManager)               (300302)) // gryzlov-ad
    ((CoordinatorSuspendEnforcment)             (300303)) // gryzlov-ad
    ((ReignInHiveMessages)                      (300304)) // ifsmirnov
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
