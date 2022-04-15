#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/ytlib/election/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/hive/public.h>

#include <yt/yt/core/misc/enum.h>
#include <yt/yt/core/misc/compact_vector.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/actions/callback.h>

namespace NYT::NHiveServer {

////////////////////////////////////////////////////////////////////////////////

using NElection::TCellId;

using NTransactionClient::TTransactionId;
using NTransactionClient::TTimestamp;
using NTransactionClient::TTransactionActionData;

using NHiveClient::TMessageId;

////////////////////////////////////////////////////////////////////////////////

struct TTransactionPrepareOptions;
struct TTransactionCommitOptions;
struct TTransactionAbortOptions;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(THiveManager)

DECLARE_ENTITY_TYPE(TMailbox, TCellId, ::THash<TCellId>)
DECLARE_REFCOUNTED_STRUCT(TMailboxRuntimeData)

constexpr int TypicalMailboxCount = 16;
using TMailboxList = TCompactVector<TMailbox*, TypicalMailboxCount>;

DECLARE_REFCOUNTED_STRUCT(TSerializedMessage)

template <class TTransaction>
using TTransactionPrepareActionHandler = TCallback<void(TTransaction*, const TString&, const TTransactionPrepareOptions&)>;
template <class TTransaction>
using TTransactionCommitActionHandler = TCallback<void(TTransaction*, const TString&, const TTransactionCommitOptions&)>;
template <class TTransaction>
using TTransactionAbortActionHandler = TCallback<void(TTransaction*, const TString&, const TTransactionAbortOptions&)>;
template <class TTransaction>
using TTransactionSerializeActionHandler = TCallback<void(TTransaction*, const TString&)>;

template <class TCallback>
struct TTransactionActionHandlerDescriptor;
template <class TTransaction>
using TTransactionPrepareActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionPrepareActionHandler<TTransaction>>;
template <class TTransaction>
using TTransactionCommitActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionCommitActionHandler<TTransaction>>;
template <class TTransaction>
using TTransactionAbortActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionAbortActionHandler<TTransaction>>;
template <class TTransaction>
using TTransactionSerializeActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionSerializeActionHandler<TTransaction>>;

DECLARE_REFCOUNTED_STRUCT(ITransactionSupervisor)
DECLARE_REFCOUNTED_STRUCT(ITransactionManager)
DECLARE_REFCOUNTED_STRUCT(ITransactionParticipantProvider)

DECLARE_REFCOUNTED_STRUCT(ITransactionLeaseTracker)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)

DECLARE_REFCOUNTED_CLASS(THiveManagerConfig)
DECLARE_REFCOUNTED_CLASS(TTransactionSupervisorConfig)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizerConfig)

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

} // namespace NYT::NHiveServer
