#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra_common/serialize.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NClusterClock {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign reign);
NHydra::EFinalRecoveryAction GetActionToRecoverFromReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EClockReign,
    ((TheBigBang)                                                 (1))  // savrus
);

////////////////////////////////////////////////////////////////////////////////

class TSaveContext
    : public NHydra::TSaveContext
{
public:
    explicit TSaveContext(NHydra::ICheckpointableOutputStream* output);

    EClockReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

class TLoadContext
    : public NHydra::TLoadContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TBootstrap*, Bootstrap);

public:
    TLoadContext(
        TBootstrap* bootstrap,
        NHydra::ICheckpointableInputStream* input);

    EClockReign GetVersion() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterClock
