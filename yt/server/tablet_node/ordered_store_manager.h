#pragma once

#include "store_manager_detail.h"
#include "dynamic_store_bits.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/tablet_client/public.h>

namespace NYT {
namespace NTabletNode {

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
        TInMemoryManagerPtr inMemoryManager = nullptr,
        NApi::INativeClientPtr client = nullptr);

    // IStoreManager overrides.
    virtual void Mount(
        const std::vector<NTabletNode::NProto::TAddStoreDescriptor>& storeDescriptors) override;

    virtual void ExecuteAtomicWrite(
        TTransaction* transaction,
        NTabletClient::TWireProtocolReader* reader,
        bool prelock) override;
    virtual void ExecuteNonAtomicWrite(
        const TTransactionId& transactionId,
        NTabletClient::TWireProtocolReader* reader) override;

    TOrderedDynamicRowRef WriteRow(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock);

    static void LockRow(TTransaction* transaction, bool prelock, const TOrderedDynamicRowRef& rowRef);
    void ConfirmRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef);
    void PrepareRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef);
    void CommitRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef);
    void AbortRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef);

    virtual bool IsStoreCompactable(IStorePtr store) const override;
    virtual bool IsStoreFlushable(IStorePtr store) const override;

    virtual IOrderedStoreManagerPtr AsOrdered() override;

private:
    TOrderedDynamicStorePtr ActiveStore_;

    virtual IDynamicStore* GetActiveStore() const override;
    virtual void ResetActiveStore() override;
    virtual void OnActiveStoreRotated() override;

    virtual TStoreFlushCallback MakeStoreFlushCallback(
        IDynamicStorePtr store,
        TTabletSnapshotPtr tabletSnapshot) override;

    virtual void CreateActiveStore() override;

    void ValidateOnWrite(const TTransactionId& transactionId, TUnversionedRow row);

};

DEFINE_REFCOUNTED_TYPE(TOrderedStoreManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
