#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/tablet_node/proto/tablet_manager.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/misc/small_vector.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletManager
    : public TRefCounted
{
public:
    TTabletManager(
        TTabletManagerConfigPtr config,
        ITabletSlotPtr slot,
        IBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();

    void Write(
        TTabletSnapshotPtr tabletSnapshot,
        TTransactionId transactionId,
        NTransactionClient::TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        int rowCount,
        size_t byteSize,
        bool versioned,
        const TSyncReplicaIdList& syncReplicaIds,
        NTableClient::TWireProtocolReader* reader,
        TFuture<void>* commitResult);

    TFuture<void> Trim(
        TTabletSnapshotPtr tabletSnapshot,
        i64 trimmedRowCount);

    void ScheduleStoreRotation(TTablet* tablet);

    TFuture<void> CommitTabletStoresUpdateTransaction(
        TTablet* tablet,
        const NApi::ITransactionPtr& transaction);

    void ReleaseBackingStore(const IChunkStorePtr& store);

    NYTree::IYPathServicePtr GetOrchidService();

    NTabletClient::ETabletCellLifeStage GetTabletCellLifeStage() const;

    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    TTablet* GetTabletOrThrow(TTabletId id);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
