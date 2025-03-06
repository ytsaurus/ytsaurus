#pragma once

#include <yt/yt/server/lib/hydra/serialize.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

NHydra::TReign GetCurrentReign();
bool ValidateSnapshotReign(NHydra::TReign reign);

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionSupervisorReign,
    ((CellIdsToSyncWithBeforePrepare)                               (10))  // babenko
    ((MaxAllowedCommitTimestamp)                                    (11))  // ifsmirnov
    ((CoordinatorPrepareMode)                                       (12))  // gritukan
    ((AbortFailedSimpleTransactions)                                (13))  // gritukan
    ((Sequencer)                                                    (14))  // aleksandra-zh
    ((SequencerFixes)                                               (15))  // aleksandra-zh
);

static_assert(
    TEnumTraits<ETransactionSupervisorReign>::IsMonotonic,
    "Transaction supervisor reign enum is not monotonic");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
