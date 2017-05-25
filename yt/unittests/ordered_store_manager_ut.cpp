#include "ordered_dynamic_store_ut_helpers.h"

#include <yt/server/tablet_node/ordered_store_manager.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NObjectClient;
using namespace NTabletClient;
using namespace NTabletClient::NProto;

////////////////////////////////////////////////////////////////////////////////

class TOrderedStoreManagerTest
    : public TStoreManagerTestBase<TOrderedDynamicStoreTestBase>
{
protected:
    virtual IStoreManagerPtr CreateStoreManager(TTablet* tablet) override
    {
        YCHECK(!StoreManager_);
        StoreManager_ = New<TOrderedStoreManager>(
            New<TTabletManagerConfig>(),
            tablet,
            this);
        return StoreManager_;
    }

    virtual IStoreManagerPtr GetStoreManager() override
    {
        return StoreManager_;
    }


    TOrderedDynamicRowRef WriteRow(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        return StoreManager_->WriteRow(transaction, row, NullTimestamp, prelock);
    }

    void WriteRow(const TUnversionedOwningRow& row)
    {
        auto transaction = StartTransaction();

        StoreManager_->WriteRow(transaction.get(), row, NullTimestamp, false);

        EXPECT_EQ(1, transaction->LockedOrderedRows().size());
        auto rowRef = transaction->LockedOrderedRows()[0];

        PrepareTransaction(transaction.get());
        StoreManager_->PrepareRow(transaction.get(), rowRef);

        CommitTransaction(transaction.get());
        StoreManager_->CommitRow(transaction.get(), rowRef);
    }

    void PrepareRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
    {
        StoreManager_->PrepareRow(transaction, rowRef);
    }

    void CommitRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
    {
        StoreManager_->CommitRow(transaction, rowRef);
    }

    void AbortRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
    {
        StoreManager_->AbortRow(transaction, rowRef);
    }

    void ConfirmRow(TTransaction* transaction, const TOrderedDynamicRowRef& rowRef)
    {
        StoreManager_->ConfirmRow(transaction, rowRef);
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

TEST_F(TOrderedStoreManagerTest, PrelockRow)
{
    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    auto rowRef = WriteRow(transaction.get(), BuildRow("a=1"), true);

    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(0, transaction->LockedOrderedRows().size());
    EXPECT_EQ(store, rowRef.Store);

    ConfirmRow(transaction.get(), rowRef);
    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(rowRef, transaction->LockedOrderedRows()[0]);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TOrderedStoreManagerTest, AbortRow)
{
    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("a=1"), false);

    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(1, store->GetLockCount());

    auto rowRef = transaction->LockedOrderedRows()[0];

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TOrderedStoreManagerTest, CommitRow)
{
    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    auto rowRef = WriteRow(transaction.get(), BuildRow("a=1"), false);

    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(store, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TOrderedStoreManagerTest, ConfirmRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    const auto* row1 = "a=1";
    WriteRow(BuildRow(row1));

    const auto* row2 = "a=2";
    auto rowRef2 = WriteRow(transaction.get(), BuildRow(row2), true);
    EXPECT_EQ(0, transaction->LockedOrderedRows().size());
    EXPECT_EQ(store1, rowRef2.Store);

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    ConfirmRow(transaction.get(), rowRef2);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(rowRef2, transaction->LockedOrderedRows()[0]);
    EXPECT_EQ(store1, rowRef2.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef2);

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef2);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1, store1->GetRowCount());
    EXPECT_EQ(1, store2->GetRowCount());

    EXPECT_TRUE(AreRowsEqual(GetRow(store1, 0), row1));
    EXPECT_TRUE(AreRowsEqual(GetRow(store2, 0), row2));
}

TEST_F(TOrderedStoreManagerTest, PrepareRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    const auto* row1 = "a=1";
    WriteRow(BuildRow(row1));

    const auto* row2 = "a=2";
    auto rowRef2 = WriteRow(transaction.get(), BuildRow(row2), false);
    EXPECT_EQ(1, transaction->LockedOrderedRows().size());

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(rowRef2, transaction->LockedOrderedRows()[0]);
    EXPECT_EQ(store1, rowRef2.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef2);
    EXPECT_EQ(store1, rowRef2.Store);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef2);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_TRUE(AreRowsEqual(GetRow(store1, 0), row1));
    EXPECT_TRUE(AreRowsEqual(GetRow(store2, 0), row2));
}

TEST_F(TOrderedStoreManagerTest, MigrateRow)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    const auto* row1 = "a=1";
    WriteRow(BuildRow(row1));

    const auto* row2 = "a=2";
    auto rowRef2 = WriteRow(transaction.get(), BuildRow(row2), false);
    EXPECT_EQ(1, transaction->LockedOrderedRows().size());

    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(rowRef2, transaction->LockedOrderedRows()[0]);
    EXPECT_EQ(store1, rowRef2.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef2);

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef2);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_TRUE(AreRowsEqual(GetRow(store1, 0), row1));
    EXPECT_TRUE(AreRowsEqual(GetRow(store2, 0), row2));
}

TEST_F(TOrderedStoreManagerTest, AbortRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    const auto* row1 = "a=1";
    WriteRow(BuildRow(row1));

    const auto* row2 = "a=2";
    auto rowRef2 = WriteRow(transaction.get(), BuildRow(row2), false);
    EXPECT_EQ(1, transaction->LockedOrderedRows().size());

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(rowRef2, transaction->LockedOrderedRows()[0]);
    EXPECT_EQ(store1, rowRef2.Store);

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), rowRef2);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1, store1->GetRowCount());
    EXPECT_EQ(0, store2->GetRowCount());
}

TEST_F(TOrderedStoreManagerTest, UnlockStoreOnCommit)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();

    const auto* row1 = "a=1";
    WriteRow(BuildRow(row1));

    const auto* row2 = "a=2";
    auto rowRef2 = WriteRow(transaction.get(), BuildRow(row2), false);

    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(rowRef2, transaction->LockedOrderedRows()[0]);
    EXPECT_EQ(store, rowRef2.Store);

    RotateStores();

    EXPECT_TRUE(StoreManager_->IsStoreLocked(store));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef2);
    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef2);

    EXPECT_FALSE(StoreManager_->IsStoreLocked(store));
}

TEST_F(TOrderedStoreManagerTest, UnlockStoreOnAbort)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();

    const auto* row1 = "a=1";
    WriteRow(BuildRow(row1));

    const auto* row2 = "a=2";
    auto rowRef2 = WriteRow(transaction.get(), BuildRow(row2), false);

    EXPECT_EQ(1, transaction->LockedOrderedRows().size());
    EXPECT_EQ(rowRef2, transaction->LockedOrderedRows()[0]);
    EXPECT_EQ(store, rowRef2.Store);

    RotateStores();

    EXPECT_TRUE(StoreManager_->IsStoreLocked(store));

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), rowRef2);

    EXPECT_FALSE(StoreManager_->IsStoreLocked(store));
}

TEST_F(TOrderedStoreManagerTest, WriteRotateWrite)
{
    auto store1 = GetActiveStore();
    EXPECT_EQ(0, store1->GetLockCount());

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    const auto* row1 = "a=1";
    auto rowRef1 = WriteRow(transaction1.get(), BuildRow(row1), false);
    EXPECT_EQ(store1, rowRef1.Store);

    const auto* row2 = "a=2";
    auto rowRef2 = WriteRow(transaction2.get(), BuildRow(row2), false);
    EXPECT_EQ(store1, rowRef2.Store);

    EXPECT_EQ(2, store1->GetLockCount());

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), rowRef2);
    CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), rowRef2);

    EXPECT_EQ(1, store1->GetLockCount());

    RotateStores();
    auto store2 = GetActiveStore();
    EXPECT_NE(store1, store2);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto transaction3 = StartTransaction();

    const auto* row3 = "a=3";
    auto rowRef3 = WriteRow(transaction3.get(), BuildRow(row3), false);
    EXPECT_EQ(store2, rowRef3.Store);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    PrepareTransaction(transaction3.get());
    PrepareRow(transaction3.get(), rowRef3);
    CommitTransaction(transaction3.get());
    CommitRow(transaction3.get(), rowRef3);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);
    CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), rowRef1);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

