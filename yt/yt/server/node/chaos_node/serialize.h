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
