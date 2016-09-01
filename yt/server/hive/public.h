#pragma once

#include <yt/server/hydra/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/core/misc/enum.h>

#include <yt/core/actions/callback.h>

namespace NYT {
namespace NHiveServer {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;

using NTransactionClient::TTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::TTransactionActionData;

using NHiveClient::TMessageId;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THiveManager)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)

DECLARE_ENTITY_TYPE(TMailbox, TCellId, ::THash<TCellId>)
DECLARE_ENTITY_TYPE(TCommit, TTransactionId, ::THash<TTransactionId>)

using TTransactionPrepareActionHandler = TCallback<void(const Stroka&, bool persistent)>;
using TTransactionCommitActionHandler = TCallback<void(const Stroka&)>;
using TTransactionAbortActionHandler = TCallback<void(const Stroka&)>;

template <class TCallback>
struct TTransactionActionHandlerDescriptor;
using TTransactionPrepareActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionPrepareActionHandler>;
using TTransactionCommitActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionCommitActionHandler>;
using TTransactionAbortActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionAbortActionHandler>;

DECLARE_REFCOUNTED_STRUCT(ITransactionManager)
DECLARE_REFCOUNTED_STRUCT(ITransactionParticipantProvider)

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
    ((Serialized)                 (6))
    ((TransientAbortPrepared)     (4))
    ((Aborted)                    (5))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHiveServer
} // namespace NYT
