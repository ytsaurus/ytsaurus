#pragma once

#include "public.h"
#include "tablet_memory_statistics.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

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

class TTabletManager
    : public TRefCounted
{
public:
    //! Raised when replication transaction is finished (committed or aborted).
    DECLARE_SIGNAL(void(TTablet*, const TTableReplicaInfo*), ReplicationTransactionFinished);
    DECLARE_SIGNAL(void(), EpochStarted);
    DECLARE_SIGNAL(void(), EpochStopped);

public:
    TTabletManager(
        TTabletManagerConfigPtr config,
        ITabletSlotPtr slot,
        IBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();
    void Finalize();

    TFuture<void> Trim(
        TTabletSnapshotPtr tabletSnapshot,
        i64 trimmedRowCount);

    void ScheduleStoreRotation(TTablet* tablet, NLsm::EStoreRotationReason reason);

    TFuture<void> CommitTabletStoresUpdateTransaction(
        TTablet* tablet,
        const NApi::ITransactionPtr& transaction);

    void ReleaseBackingStore(const IChunkStorePtr& store);

    NYTree::IYPathServicePtr GetOrchidService();

    NTabletClient::ETabletCellLifeStage GetTabletCellLifeStage() const;

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    TTablet* GetTabletOrThrow(TTabletId id);

    ITabletCellWriteManagerHostPtr GetTabletCellWriteManagerHost();

    std::vector<TTabletMemoryStatistics> GetMemoryStatistics() const;

    void UpdateTabletSnapshot(TTablet* tablet, std::optional<TLockManagerEpoch> epoch = std::nullopt);

    bool AllocateDynamicStoreIfNeeded(TTablet* tablet);

    // COMPAT(aleksandra-zh)
    void RestoreHunkLocks(
        TTransaction* transaction,
        NTabletServer::NProto::TReqUpdateTabletStores* request);
    void ValidateHunkLocks();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
