#pragma once

#include "public.h"
#include "tablet_memory_statistics.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/lsm/public.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

// COMPAT(aleksandra-zh)
#include <yt/yt/server/lib/tablet_server/proto/tablet_manager.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct ITabletManager
    : public virtual TRefCounted
{
    //! Raised when replication transaction is finished (committed or aborted).
    DECLARE_INTERFACE_SIGNAL(void(TTablet*, const TTableReplicaInfo*), ReplicationTransactionFinished);
    DECLARE_INTERFACE_SIGNAL(void(), EpochStarted);
    DECLARE_INTERFACE_SIGNAL(void(), EpochStopped);

    virtual void Initialize() = 0;
    virtual void Finalize() = 0;

    virtual TFuture<void> Trim(
        const TTabletSnapshotPtr& tabletSnapshot,
        i64 trimmedRowCount) = 0;

    virtual void ScheduleStoreRotation(TTablet* tablet, NLsm::EStoreRotationReason reason) = 0;

    virtual TFuture<void> CommitTabletStoresUpdateTransaction(
        TTablet* tablet,
        const NApi::ITransactionPtr& transaction) = 0;

    virtual void ReleaseBackingStore(const IChunkStorePtr& store) = 0;

    virtual NYTree::IYPathServicePtr GetOrchidService() = 0;

    virtual NTabletClient::ETabletCellLifeStage GetTabletCellLifeStage() const = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    virtual TTablet* GetTabletOrThrow(TTabletId id) = 0;

    virtual ITabletCellWriteManagerHostPtr GetTabletCellWriteManagerHost() = 0;

    virtual std::vector<TTabletMemoryStatistics> GetMemoryStatistics() const = 0;

    virtual void UpdateTabletSnapshot(
        TTablet* tablet,
        std::optional<TLockManagerEpoch> epoch = std::nullopt) = 0;

    virtual bool AllocateDynamicStoreIfNeeded(TTablet* tablet) = 0;

    // COMPAT(aleksandra-zh)
    virtual void RestoreHunkLocks(
        TTransaction* transaction,
        NTabletServer::NProto::TReqUpdateTabletStores* request) = 0;
    virtual void ValidateHunkLocks() = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletManager)

////////////////////////////////////////////////////////////////////////////////

ITabletManagerPtr CreateTabletManager(
    TTabletManagerConfigPtr config,
    ITabletSlotPtr slot,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
