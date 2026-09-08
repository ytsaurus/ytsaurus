#pragma once

#include "public.h"

#include <yt/yt/server/lib/hydra/serialize.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionSupervisorReign,
    ((Sequencer)                                                    (14))  // aleksandra-zh
    ((SequencerFixes)                                               (15))  // aleksandra-zh
    ((SaveLastCoordinatorCommitTimestamp)                           (16))  // aleksandra-zh
    ((StrongOrderingTags)                                           (17))  // h0pless
    ((ExpectedPrepareSignature)                                     (18))  // atalmenev
    ((StopSendingUnnecessaryRequests)                               (19))  // h0pless
    ((RemoveUnusedAliases)                                          (20))  // h0pless
    ((RenameReadyToCommit)                                          (21))  // h0pless
);

static_assert(
    TEnumTraits<ETransactionSupervisorReign>::IsMonotonic,
    "Transaction supervisor reign enum is not monotonic");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
