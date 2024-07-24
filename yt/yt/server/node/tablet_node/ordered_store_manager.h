#pragma once

#include "store_manager_detail.h"
#include "dynamic_store_bits.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/table_client/public.h>

#include <yt/yt/ytlib/tablet_client/public.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

class TOrderedStoreManager
    : public TStoreManagerBase
    , public IOrderedStoreManager
{
public:
    TOrderedStoreManager(
        TTabletManagerConfigPtr config,
        TTablet* tablet,
        ITabletContext* tabletContext,
        NHydra::IHydraManagerPtr hydraManager = nullptr,
        IInMemoryManagerPtr inMemoryManager = nullptr,
        NApi::NNative::IClientPtr client = nullptr);

    // IStoreManager overrides.
    void Mount(
        TRange<const NTabletNode::NProto::TAddStoreDescriptor*> storeDescriptors,
        TRange<const NTabletNode::NProto::TAddHunkChunkDescriptor*> hunkChunkDescriptors,
        bool createDynamicStore,
        const NTabletNode::NProto::TMountHint& mountHint) override;

    void LockHunkStores(TWriteContext* context) override;
    bool ExecuteWrites(
        NTableClient::IWireProtocolReader* reader,
        TWriteContext* context) override;

    void UpdateCommittedStoreRowCount() override;

    TOrderedDynamicRowRef WriteRow(
        TUnversionedRow row,
        TWriteContext* context);

    void CreateActiveStore(TDynamicStoreId hintId = {}) override;
    void DiscardAllStores() override;

    bool IsFlushNeeded() const override;
    bool IsStoreCompactable(IStorePtr store) const override;
    bool IsStoreFlushable(IStorePtr store) const override;

    IOrderedStoreManagerPtr AsOrdered() override;

private:
    TOrderedDynamicStorePtr ActiveStore_;

    IDynamicStore* GetActiveStore() const override;
    void ResetActiveStore() override;
    void OnActiveStoreRotated() override;

    TStoreFlushCallback MakeStoreFlushCallback(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot,
        bool isUnmountWorkflow) override;

    i64 ComputeStartingRowIndex() const;
};

DEFINE_REFCOUNTED_TYPE(TOrderedStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
