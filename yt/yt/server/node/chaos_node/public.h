#pragma once

#include <yt/yt/server/lib/chaos_node/public.h>

#include <yt/yt/server/lib/hive/public.h>

#include <yt/yt/server/lib/hydra/public.h>

#include <yt/yt/client/transaction_client/public.h>

#include <yt/yt/client/chaos_client/replication_card.h>

#include <yt/yt/core/misc/serialize.h>

#include <library/cpp/yt/misc/enum.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap;

DECLARE_REFCOUNTED_CLASS(TChaosAutomaton)
DECLARE_REFCOUNTED_STRUCT(ISlotManager)
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
using TPersistenceContext = TCustomPersistenceContext<TSaveContext, TLoadContext, EChaosSnapshotVersion>;

DEFINE_ENUM(EAutomatonThreadQueue,
    (Default)
    (Mutation)
    (EraCommencer)
    (ReplicatedTableTracker)
    (MigrationDepartment)
);

using TReplicationCardId = NChaosClient::TReplicationCardId;
DECLARE_ENTITY_TYPE(TReplicationCard, NChaosClient::TReplicationCardId, NObjectClient::TDirectObjectIdHash)

using TReplicationCardCollocationId = NChaosClient::TReplicationCardCollocationId;
DECLARE_ENTITY_TYPE(TReplicationCardCollocation, NChaosClient::TReplicationCardCollocationId, NObjectClient::TDirectObjectIdHash)

using TTransactionId = NTransactionClient::TTransactionId;
DECLARE_ENTITY_TYPE(TTransaction, TTransactionId, ::THash<TTransactionId>)

using TTimestamp = NTransactionClient::TTimestamp;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
