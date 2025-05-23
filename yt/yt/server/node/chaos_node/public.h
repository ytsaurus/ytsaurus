#pragma once

#include <yt/yt/server/lib/chaos_node/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/server/lib/transaction_supervisor/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_CLASS(TChaosAutomaton)
DECLARE_REFCOUNTED_STRUCT(ISlotManager)
DECLARE_REFCOUNTED_STRUCT(IChaosAutomatonHost)
DECLARE_REFCOUNTED_STRUCT(IChaosSlot)
DECLARE_REFCOUNTED_STRUCT(IChaosManager)
DECLARE_REFCOUNTED_STRUCT(ICoordinatorManager)
DECLARE_REFCOUNTED_STRUCT(ITransactionManager)
DECLARE_REFCOUNTED_STRUCT(IChaosCellSynchronizer)
DECLARE_REFCOUNTED_STRUCT(IShortcutSnapshotStore)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardObserver)
DECLARE_REFCOUNTED_STRUCT(IMigratedReplicationCardRemover)
DECLARE_REFCOUNTED_STRUCT(IForeignMigratedReplicationCardRemover)

enum class EChaosSnapshotVersion;

class TSaveContext;
class TLoadContext;
using TPersistenceContext = TCustomPersistenceContext<
    TSaveContext,
    TLoadContext,
    EChaosSnapshotVersion
>;

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (Mutation)
    (EraCommencer)
    (ReplicatedTableTracker)
    (MigrationDepartment)
);

class TChaosObjectBase;

using TChaosObjectId = NChaosClient::TChaosObjectId;

using TReplicationCardId = NChaosClient::TReplicationCardId;
DECLARE_ENTITY_TYPE(TReplicationCard, TReplicationCardId, NObjectClient::TObjectIdEntropyHash)

using TReplicationCardCollocationId = NChaosClient::TReplicationCardCollocationId;
DECLARE_ENTITY_TYPE(TReplicationCardCollocation, TReplicationCardCollocationId, NObjectClient::TObjectIdEntropyHash)

using TTransactionId = NTransactionClient::TTransactionId;
DECLARE_ENTITY_TYPE(TTransaction, TTransactionId, ::THash<TTransactionId>)

using TChaosLeaseId = NChaosClient::TChaosLeaseId;
DECLARE_ENTITY_TYPE(TChaosLease, TChaosLeaseId, NObjectClient::TObjectIdEntropyHash)

using TTimestamp = NTransactionClient::TTimestamp;

template <class TProto, class TState>
using TTypedTransactionActionDescriptor = NTransactionSupervisor::TTypedTransactionActionDescriptor<
    TTransaction,
    TProto,
    TState
>;

using TTypeErasedTransactionActionDescriptor = NTransactionSupervisor::TTypeErasedTransactionActionDescriptor<
    TTransaction,
    TSaveContext,
    TLoadContext
>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
