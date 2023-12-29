#pragma once

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/actions/callback.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

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

DECLARE_REFCOUNTED_STRUCT(ITransactionLeaseTracker)

DECLARE_REFCOUNTED_CLASS(TTransactionSupervisorConfig)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETransactionState,
    ((Active)                     (0))
    ((TransientCommitPrepared)    (1))
    ((PersistentCommitPrepared)   (2))
    ((CommitPending)              (7))
    ((Committed)                  (3))
    ((Serialized)                 (6))
    ((TransientAbortPrepared)     (4))
    ((Aborted)                    (5))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionSupervisor
