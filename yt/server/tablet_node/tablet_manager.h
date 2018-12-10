#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/server/tablet_node/tablet_manager.pb.h>

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/client/api/public.h>

#include <yt/core/misc/small_vector.h>

#include <yt/core/ytree/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TTabletManager
    : public TRefCounted
{
public:
    TTabletManager(
        TTabletManagerConfigPtr config,
        TTabletSlotPtr slot,
        NCellNode::TBootstrap* bootstrap);
    ~TTabletManager();

    void Initialize();


    void Read(
        TTabletSnapshotPtr tabletSnapshot,
        TTimestamp timestamp,
        const TString& user,
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        NTableClient::TRetentionConfigPtr retentionConfig,
        NTableClient::TWireProtocolReader* reader,
        NTableClient::TWireProtocolWriter* writer);

    void Write(
        TTabletSnapshotPtr tabletSnapshot,
        const TTransactionId& transactionId,
        NTransactionClient::TTimestamp transactionStartTimestamp,
        TDuration transactionTimeout,
        TTransactionSignature signature,
        int rowCount,
        size_t byteSize,
        const TString& user,
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

    NYTree::IYPathServicePtr GetOrchidService();

    i64 GetDynamicStoresMemoryUsage() const;
    i64 GetStaticStoresMemoryUsage() const;
    i64 GetWriteLogsMemoryUsage() const;


    DECLARE_ENTITY_MAP_ACCESSORS(Tablet, TTablet);
    TTablet* GetTabletOrThrow(const TTabletId& id);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TTabletManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
