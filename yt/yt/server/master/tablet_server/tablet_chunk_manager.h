#pragma once

#include "public.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/table_server/public.h>

#include <yt/yt/server/master/chunk_server/public.h>

#include <yt/yt/server/master/transaction_server/public.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

struct ITabletChunkManager
    : public virtual TRefCounted
{
    virtual void CopyChunkListsIfShared(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        bool force = false) = 0;

    virtual void ReshardTable(
        NTableServer::TTableNode* table,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount,
        const std::vector<NTableClient::TLegacyOwningKey>& oldPivotKeys,
        const std::vector<NTableClient::TLegacyOwningKey>& newPivotKeys,
        const THashSet<TStoreId>& oldEdenStoreIds) = 0;

    virtual void ReshardHunkStorage(
        THunkStorageNode* hunkStorage,
        int firstTabletIndex,
        int lastTabletIndex,
        int newTabletCount) = 0;

    virtual void PrepareUpdateTabletStores(
        TTablet* tablet,
        NProto::TReqUpdateTabletStores* request) = 0;

    //! Returns logging string containing update statistics.
    virtual TString CommitUpdateTabletStores(
        TTablet* tablet,
        NTransactionServer::TTransaction* transaction,
        NProto::TReqUpdateTabletStores* request,
        NTabletClient::ETabletStoresUpdateReason updateReason) = 0;

    //! Returns logging string containing update statistics.
    virtual TString CommitUpdateHunkTabletStores(
        THunkTablet* tablet,
        NProto::TReqUpdateHunkTabletStores* request) = 0;

    virtual void MakeTableDynamic(NTableServer::TTableNode* table) = 0;
    virtual void MakeTableStatic(NTableServer::TTableNode* table) = 0;

    virtual void SetTabletEdenStoreIds(
        TTablet* tablet,
        std::vector<TStoreId> edenStoreIds) = 0;

    virtual void DetachChunksFromTablet(
        TTabletBase* tablet,
        const std::vector<NChunkServer::TChunkTree*>& chunkTrees,
        NChunkServer::EChunkDetachPolicy policy) = 0;

    virtual void WrapWithBackupChunkViews(
        TTablet* tablet,
        NTransactionClient::TTimestamp maxClipTimestamp) = 0;

    virtual TError PromoteFlushedDynamicStores(TTablet* tablet) = 0;

    virtual TError ApplyBackupCutoff(TTablet* tablet) = 0;

    virtual NChunkServer::TDynamicStore* CreateDynamicStore(
        TTablet* tablet,
        NTabletClient::TDynamicStoreId hintId = {}) = 0;
};

DEFINE_REFCOUNTED_TYPE(ITabletChunkManager)

////////////////////////////////////////////////////////////////////////////////

ITabletChunkManagerPtr CreateTabletChunkManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
