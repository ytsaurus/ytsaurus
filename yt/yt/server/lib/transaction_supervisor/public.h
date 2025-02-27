#pragma once

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NTransactionSupervisor {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;

using NTransactionClient::TTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::TTransactionActionData;

////////////////////////////////////////////////////////////////////////////////

struct TTransactionPrepareOptions;
struct TTransactionCommitOptions;
struct TTransactionAbortOptions;

namespace NProto {

class TTransactionPrepareOptions;
class TTransactionCommitOptions;
class TTransactionAbortOptions;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(ITransactionSupervisor)
DECLARE_REFCOUNTED_STRUCT(ITransactionManager)
DECLARE_REFCOUNTED_STRUCT(ITransactionParticipantProvider)
DECLARE_REFCOUNTED_STRUCT(ITransactionLeaseTrackerThreadPool)
DECLARE_REFCOUNTED_STRUCT(ITransactionLeaseTracker)

DECLARE_REFCOUNTED_STRUCT(TTransactionSupervisorConfig)
DECLARE_REFCOUNTED_STRUCT(TTransactionLeaseTrackerConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    ((Active)                     (0))
    ((TransientCommitPrepared)    (1))
    ((PersistentCommitPrepared)   (2))
    ((CommitPending)              (7))
    ((Committed)                  (3))
    // If some serialization is needed than transaction will go through Serialized.
    // Transaction in Serialized state will not finish until all serializations are completed.
    // Finish of a coarse serialization will always trigger transition to Serialized.
    //
    // NB: There is no real checks or events connected to this state.
    ((Serialized)                 (6))
    ((TransientAbortPrepared)     (4))
    ((Aborted)                    (5))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
