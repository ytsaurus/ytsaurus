#pragma once

#include <yt/server/hydra/public.h>

#include <yt/ytlib/election/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/core/misc/enum.h>
#include <yt/core/misc/small_vector.h>
#include <yt/core/misc/protobuf_helpers.h>

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

DECLARE_ENTITY_TYPE(TMailbox, TCellId, ::THash<TCellId>)

constexpr int TypicalMailboxCount = 16;
using TMailboxList = SmallVector<TMailbox*, TypicalMailboxCount>;

using TRefCountedEncapsulatedMessage = TRefCountedProto<NHiveClient::NProto::TEncapsulatedMessage>;
using TRefCountedEncapsulatedMessagePtr = TIntrusivePtr<TRefCountedEncapsulatedMessage>;

template <class TTransaction>
using TTransactionPrepareActionHandler = TCallback<void(TTransaction*, const TString&, bool persistent)>;
template <class TTransaction>
using TTransactionCommitActionHandler = TCallback<void(TTransaction*, const TString&)>;
template <class TTransaction>
using TTransactionAbortActionHandler = TCallback<void(TTransaction*, const TString&)>;

template <class TCallback>
struct TTransactionActionHandlerDescriptor;
template <class TTransaction>
using TTransactionPrepareActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionPrepareActionHandler<TTransaction>>;
template <class TTransaction>
using TTransactionCommitActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionCommitActionHandler<TTransaction>>;
template <class TTransaction>
using TTransactionAbortActionHandlerDescriptor = TTransactionActionHandlerDescriptor<TTransactionAbortActionHandler<TTransaction>>;

DECLARE_REFCOUNTED_STRUCT(ITransactionManager)
DECLARE_REFCOUNTED_STRUCT(ITransactionParticipantProvider)

DECLARE_REFCOUNTED_CLASS(TTransactionSupervisor)
DECLARE_REFCOUNTED_CLASS(TTransactionLeaseTracker)
DECLARE_REFCOUNTED_CLASS(TCellDirectorySynchronizer)
DECLARE_REFCOUNTED_CLASS(TClusterDirectorySynchronizer)

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

} // namespace NHiveServer
} // namespace NYT
