#include "stdafx.h"
#include "memory_store_ut.h"

#include <ytlib/tablet_client/wire_protocol.h>

#include <server/tablet_node/store_manager.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TStoreManagerTest
    : public TMemoryStoreTestBase
{
protected:
    TStoreManagerTest()
        : TMemoryStoreTestBase()
        , StoreManager(New<TStoreManager>(
            New<TTabletManagerConfig>(),
            Tablet.get()))
    {
        StoreManager->CreateActiveStore();
    }

    void Rotate()
    {
        StoreManager->SetRotationScheduled();
        StoreManager->RotateStores(true);
    }

    void WriteRow(const TUnversionedOwningRow& row)
    {
        auto transaction = StartTransaction();

        StoreManager->WriteRow(transaction.get(), row.Get(), false);

        EXPECT_EQ(1, transaction->LockedRows().size());
        auto rowRef = transaction->LockedRows()[0];

        PrepareTransaction(transaction.get());
        StoreManager->PrepareRow(rowRef);

        CommitTransaction(transaction.get());
        StoreManager->CommitRow(rowRef);
    }

    void DeleteRow(const TOwningKey& key)
    {
        auto transaction = StartTransaction();

        StoreManager->DeleteRow(transaction.get(), key.Get(), false);

        EXPECT_EQ(1, transaction->LockedRows().size());
        auto rowRef = transaction->LockedRows()[0];

        PrepareTransaction(transaction.get());
        StoreManager->PrepareRow(rowRef);

        CommitTransaction(transaction.get());
        StoreManager->CommitRow(rowRef);
    }

    using TMemoryStoreTestBase::LookupRow;
    
    TUnversionedOwningRow LookupRow(const TOwningKey& key, TTimestamp timestamp)
    {
        TSharedRef request;
        {
            TWireProtocolWriter writer;
            writer.WriteColumnFilter(TColumnFilter());
            std::vector<TUnversionedRow> keys(1, key.Get());
            writer.WriteUnversionedRowset(keys);
            request = MergeRefs(writer.Flush());
        }

        TSharedRef response;
        {
            TWireProtocolReader reader(request);
            TWireProtocolWriter writer;
            StoreManager->LookupRows(timestamp, &reader, &writer);
            response = MergeRefs(writer.Flush());
        }

        {
            TWireProtocolReader reader(response);
            std::vector<TUnversionedRow> rows;
            reader.ReadUnversionedRowset(&rows);
            EXPECT_EQ(1, rows.size());
            return TUnversionedOwningRow(rows[0]);
        }
    }


    TStoreManagerPtr StoreManager;

};

///////////////////////////////////////////////////////////////////////////////

TEST_F(TStoreManagerTest, PrelockRow)
{
    auto store = Tablet->GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    auto rowRef = StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), true);

    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(0, transaction->LockedRows().size());
    EXPECT_EQ(store, rowRef.Store);

    StoreManager->ConfirmRow(rowRef);
    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_TRUE(transaction->LockedRows()[0] == rowRef);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TStoreManagerTest, AbortRow)
{
    auto store = Tablet->GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);

    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_EQ(1, store->GetLockCount());

    auto rowRef = transaction->LockedRows()[0];

    AbortTransaction(transaction.get());
    StoreManager->AbortRow(rowRef);

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TStoreManagerTest, CommitRow)
{
    auto store = Tablet->GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    auto rowRef = StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);

    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(store, rowRef.Store);

    PrepareTransaction(transaction.get());
    StoreManager->PrepareRow(rowRef);

    CommitTransaction(transaction.get());
    StoreManager->CommitRow(rowRef);

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TStoreManagerTest, ConfirmRowWithRotation)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    auto rowRef1 = StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), true);
    EXPECT_EQ(0, transaction->LockedRows().size());
    EXPECT_EQ(store1, rowRef1.Store);

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    StoreManager->ConfirmRow(rowRef1);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto rowRef2 = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef2.Store == store1);

    PrepareTransaction(transaction.get());
    StoreManager->PrepareRow(rowRef2);

    CommitTransaction(transaction.get());
    StoreManager->CommitRow(rowRef2);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=1"));
}

TEST_F(TStoreManagerTest, PrepareRowWithRotation)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);
    EXPECT_EQ(1, transaction->LockedRows().size());

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    PrepareTransaction(transaction.get());
    StoreManager->PrepareRow(rowRef);
    EXPECT_TRUE(rowRef.Store == store1);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction.get());
    StoreManager->CommitRow(rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=1"));
}

TEST_F(TStoreManagerTest, MigrateRowOnCommit)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);
    EXPECT_EQ(1, transaction->LockedRows().size());

    auto& rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    PrepareTransaction(transaction.get());
    StoreManager->PrepareRow(rowRef);

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction.get());
    StoreManager->CommitRow(rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=1"));
}

TEST_F(TStoreManagerTest, OverwriteRowWithRotation1)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);
    EXPECT_EQ(1, transaction->LockedRows().size());

    auto rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=2").Get(), false);
    EXPECT_EQ(1, transaction->LockedRows().size());

    EXPECT_TRUE(rowRef.Store == store1);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    PrepareTransaction(transaction.get());
    StoreManager->PrepareRow(rowRef);

    CommitTransaction(transaction.get());
    StoreManager->CommitRow(rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=2"));
}

TEST_F(TStoreManagerTest, OverwriteRowWithRotation2)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    auto rowRef = StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), true);
    EXPECT_EQ(0, transaction->LockedRows().size());
    EXPECT_EQ(store1, rowRef.Store);

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_FALSE(StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), true));
    EXPECT_EQ(0, transaction->LockedRows().size());

    StoreManager->ConfirmRow(rowRef);
    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_EQ(0, rowRef.Row.GetLockIndex());

    PrepareTransaction(transaction.get());
    StoreManager->PrepareRow(rowRef);

    CommitTransaction(transaction.get());
    StoreManager->CommitRow(rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=1"));
}

TEST_F(TStoreManagerTest, WriteAfterDeleteFailureWithRotation)
{
    auto transaction = StartTransaction();

    StoreManager->DeleteRow(transaction.get(), BuildKey("1").Get(), true);

    Rotate();

    ASSERT_ANY_THROW({
        StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=2").Get(), true);
    });
}

TEST_F(TStoreManagerTest, WriteWriteConflictWithRotation1)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    StoreManager->WriteRow(transaction1.get(), BuildRow("key=1;a=1").Get(), true);

    Rotate();

    ASSERT_ANY_THROW({
        StoreManager->WriteRow(transaction2.get(), BuildRow("key=1;a=1").Get(), true);
    });
}

TEST_F(TStoreManagerTest, WriteWriteConflictWithRotation2)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    StoreManager->WriteRow(transaction1.get(), BuildRow("key=1;a=1").Get(), false);
    
    EXPECT_EQ(1, transaction1->LockedRows().size());
    auto rowRef1 = transaction1->LockedRows()[0];

    PrepareTransaction(transaction1.get());
    StoreManager->PrepareRow(rowRef1);

    CommitTransaction(transaction1.get());
    StoreManager->CommitRow(rowRef1);

    Rotate();

    ASSERT_ANY_THROW({
        StoreManager->WriteRow(transaction2.get(), BuildRow("key=1;a=1").Get(), true);
    });
}

TEST_F(TStoreManagerTest, WriteWriteConflictWithRotation3)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto store1 = Tablet->GetActiveStore();

    StoreManager->WriteRow(transaction1.get(), BuildRow("key=1;a=1").Get(), true);

    Rotate();

    StoreManager->RemoveStore(store1);

    ASSERT_ANY_THROW({
        StoreManager->WriteRow(transaction2.get(), BuildRow("key=1;a=1").Get(), true);
    });
}

TEST_F(TStoreManagerTest, AbortRowWithRotation)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);
    EXPECT_EQ(1, transaction->LockedRows().size());

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto& rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    AbortTransaction(transaction.get());
    StoreManager->AbortRow(rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Null);
}

TEST_F(TStoreManagerTest, LookupRow1)
{
    WriteRow(BuildRow("key=1;a=100", false));
    Rotate();
    WriteRow(BuildRow("key=1;b=3.14", false));
    CompareRows(LookupRow(BuildKey("1"), LastCommittedTimestamp), Stroka("key=1;a=100;b=3.14"));
}

TEST_F(TStoreManagerTest, LookupRow2)
{
    WriteRow(BuildRow("key=1;a=100", false));
    DeleteRow(BuildKey("1"));
    Rotate();
    WriteRow(BuildRow("key=1;b=3.14", false));
    CompareRows(LookupRow(BuildKey("1"), LastCommittedTimestamp), Stroka("key=1;b=3.14"));
}

TEST_F(TStoreManagerTest, LookupRow3)
{
    WriteRow(BuildRow("key=1;a=100", false));
    Rotate();
    DeleteRow(BuildKey("1"));
    WriteRow(BuildRow("key=1;b=3.14", false));
    CompareRows(LookupRow(BuildKey("1"), LastCommittedTimestamp), Stroka("key=1;b=3.14"));
}

TEST_F(TStoreManagerTest, LookupRow4)
{
    WriteRow(BuildRow("key=1;a=100", false));
    Rotate();
    WriteRow(BuildRow("key=1;b=3.14", false));
    Rotate();
    WriteRow(BuildRow("key=1;a=200;c=test", false));
    Rotate();
    CompareRows(LookupRow(BuildKey("1"), LastCommittedTimestamp), Stroka("key=1;a=200;b=3.14;c=test"));
}

TEST_F(TStoreManagerTest, UnlockStoreOnCommit)
{
    auto store = Tablet->GetActiveStore();
    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);
    EXPECT_EQ(1, transaction->LockedRows().size());
    auto rowRef = transaction->LockedRows()[0];

    Rotate();

    EXPECT_TRUE(StoreManager->IsStoreLocked(store));

    PrepareTransaction(transaction.get());
    StoreManager->PrepareRow(rowRef);
    CommitTransaction(transaction.get());
    StoreManager->CommitRow(rowRef);

    EXPECT_FALSE(StoreManager->IsStoreLocked(store));
}

TEST_F(TStoreManagerTest, UnlockStoreOnAbort)
{
    auto store = Tablet->GetActiveStore();
    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false);
    EXPECT_EQ(1, transaction->LockedRows().size());
    auto rowRef = transaction->LockedRows()[0];

    Rotate();

    EXPECT_TRUE(StoreManager->IsStoreLocked(store));

    AbortTransaction(transaction.get());
    StoreManager->AbortRow(rowRef);

    EXPECT_FALSE(StoreManager->IsStoreLocked(store));
}

TEST_F(TStoreManagerTest, WriteRotateWrite)
{
    auto store1 = Tablet->GetActiveStore();
    EXPECT_EQ(0, store1->GetLockCount());

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto rowRef1 = StoreManager->WriteRow(transaction1.get(), BuildRow("key=1;a=1").Get(), false);
    EXPECT_EQ(store1, rowRef1.Store);

    auto rowRef2 = StoreManager->WriteRow(transaction2.get(), BuildRow("key=2;a=2").Get(), false);
    EXPECT_EQ(store1, rowRef2.Store);

    EXPECT_EQ(2, store1->GetLockCount());

    PrepareTransaction(transaction2.get());
    StoreManager->PrepareRow(rowRef2);
    CommitTransaction(transaction2.get());
    StoreManager->CommitRow(rowRef2);

    EXPECT_EQ(1, store1->GetLockCount());

    Rotate();
    auto store2 = Tablet->GetActiveStore();
    EXPECT_TRUE(store1 != store2);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto transaction3 = StartTransaction();

    auto rowRef3 = StoreManager->WriteRow(transaction3.get(), BuildRow("key=2;a=3").Get(), false);
    EXPECT_EQ(store2, rowRef3.Store);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    PrepareTransaction(transaction3.get());
    StoreManager->PrepareRow(rowRef3);
    CommitTransaction(transaction3.get());
    StoreManager->CommitRow(rowRef3);

    PrepareTransaction(transaction1.get());
    StoreManager->PrepareRow(rowRef1);
    CommitTransaction(transaction1.get());
    StoreManager->CommitRow(rowRef1);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

