#pragma once

#include <yt/server/hydra/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/core/misc/enum.h>

namespace NYT {
namespace NHive {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THiveManager)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)

DECLARE_ENTITY_TYPE(TMailbox, NElection::TCellId, ::THash<NElection::TCellId>)

DECLARE_REFCOUNTED_STRUCT(ITransactionManager)

DECLARE_REFCOUNTED_CLASS(TTransactionSupervisor)
DECLARE_REFCOUNTED_CLASS(TTransactionLeaseTracker)

DECLARE_REFCOUNTED_CLASS(THiveManagerConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionSupervisorConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    ((Active)                     (0))
    ((TransientCommitPrepared)    (1))
    ((PersistentCommitPrepared)   (2))
    ((Committed)                  (3))
    ((TransientAbortPrepared)     (4))
    ((Aborted)                    (5))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHive
} // namespace NYT
