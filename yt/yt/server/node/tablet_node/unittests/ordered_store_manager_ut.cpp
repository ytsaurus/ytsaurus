#include "ordered_dynamic_store_ut_helpers.h"

#include <yt/yt/server/node/tablet_node/ordered_store_manager.h>

namespace NYT::NTabletNode {
namespace {

using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TOrderedStoreManagerTest
    : public TStoreManagerTestBase<TOrderedDynamicStoreTestBase>
{
protected:
    IStoreManagerPtr CreateStoreManager(TTablet* tablet) override
    {
        YT_VERIFY(!StoreManager_);
        StoreManager_ = New<TOrderedStoreManager>(
            New<TTabletManagerConfig>(),
            tablet,
            &TabletContext_);
        return StoreManager_;
    }

    IStoreManagerPtr GetStoreManager() override
    {
        return StoreManager_;
    }


    TOrderedDynamicRowRef WriteRow(
        TTransaction* transaction,
        TUnversionedRow row)
    {
        TWriteContext context;
        context.Phase = EWritePhase::Commit;
        context.Transaction = transaction;
        return StoreManager_->WriteRow(row, &context);
    }

    TOrderedDynamicRowRef WriteRow(const TUnversionedOwningRow& row)
    {
        auto transaction = StartTransaction();

        auto rowRef = WriteRow(transaction.get(), row);

        PrepareTransaction(transaction.get());
        CommitTransaction(transaction.get());

        Tablet_->UpdateTotalRowCount();

        return rowRef;
    }

    using TOrderedDynamicStoreTestBase::GetRow;

    TUnversionedOwningRow GetRow(i64 index)
    {
        return GetRow(GetActiveStore(), index);
    }

    TOrderedDynamicStorePtr GetActiveStore()
    {
        return Tablet_->GetActiveStore()->AsOrderedDynamic();
    }


    TOrderedStoreManagerPtr StoreManager_;

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedStoreManagerTest, WriteRows)
{
    // Not much to test here, in fact.
    auto store = GetActiveStore();

    for (int i = 0; i < 100; ++i) {
        auto row = Format("a=%v", i);
        auto rowRef = WriteRow(BuildRow(row));
        EXPECT_EQ(store, rowRef.Store);
        GetStoreManager()->UpdateCommittedStoreRowCount();
        EXPECT_TRUE(AreRowsEqual(GetRow(i), row));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

