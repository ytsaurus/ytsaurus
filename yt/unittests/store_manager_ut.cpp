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
        StoreManager->Rotate(true);
    }

    void WriteRow(const TUnversionedOwningRow& row)
    {
        auto transaction = StartTransaction();

        StoreManager->WriteRow(transaction.get(), row.Get(), false, nullptr);

        EXPECT_EQ(1, transaction->LockedRows().size());
        const auto& rowRef = transaction->LockedRows()[0];

        PrepareTransaction(transaction.get());
        StoreManager->PrepareRow(rowRef);

        CommitTransaction(transaction.get());
        StoreManager->CommitRow(rowRef);
    }

    void DeleteRow(const TOwningKey& key)
    {
        auto transaction = StartTransaction();

        StoreManager->DeleteRow(transaction.get(), key.Get(), false, nullptr);

        EXPECT_EQ(1, transaction->LockedRows().size());
        const auto& rowRef = transaction->LockedRows()[0];

        PrepareTransaction(transaction.get());
        StoreManager->PrepareRow(rowRef);

        CommitTransaction(transaction.get());
        StoreManager->CommitRow(rowRef);
    }

    using TMemoryStoreTestBase::LookupRow;
    
    TUnversionedOwningRow LookupRow(const TOwningKey& key, TTimestamp timestamp)
    {
        Stroka request;
        {
            TWireProtocolWriter writer;
            writer.WriteColumnFilter(TColumnFilter());
            std::vector<TUnversionedRow> keys(1, key.Get());
            writer.WriteUnversionedRowset(keys);
            request = writer.Finish();
        }
        
        Stroka response;
        {
            TWireProtocolReader reader(request);
            TWireProtocolWriter writer;
            StoreManager->LookupRows(timestamp, &reader, &writer);
            response = writer.Finish();
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

TEST_F(TStoreManagerTest, PrewriteRow)
{
    auto* store = Tablet->GetActiveStore().Get();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    std::vector<TDynamicRow> lockedRows;
    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), true, &lockedRows);

    EXPECT_EQ(1, lockedRows.size());
    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(0, transaction->LockedRows().size());

    TDynamicRowRef rowRef(store, lockedRows[0]);

    StoreManager->ConfirmRow(rowRef);
    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_TRUE(transaction->LockedRows()[0] == rowRef);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TStoreManagerTest, AbortRow)
{
    auto* store = Tablet->GetActiveStore().Get();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false, nullptr);

    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_EQ(1, store->GetLockCount());

    const auto& rowRef = transaction->LockedRows()[0];

    StoreManager->AbortRow(rowRef);
    AbortTransaction(transaction.get());

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TStoreManagerTest, CommitRow)
{
    auto* store = Tablet->GetActiveStore().Get();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    std::vector<TDynamicRow> lockedRows;
    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false, &lockedRows);

    EXPECT_EQ(1, lockedRows.size());
    EXPECT_EQ(1, store->GetLockCount());

    TDynamicRowRef rowRef(store, lockedRows[0]);

    StoreManager->PrepareRow(rowRef);
    PrepareTransaction(transaction.get());

    StoreManager->CommitRow(rowRef);
    CommitTransaction(transaction.get());

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TStoreManagerTest, MigrateRowOnConfirm)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    std::vector<TDynamicRow> lockedRows;
    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), true, &lockedRows);
    EXPECT_EQ(1, lockedRows.size());
    EXPECT_EQ(0, transaction->LockedRows().size());

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    TDynamicRowRef rowRef1(store1.Get(), lockedRows[0]);
    StoreManager->ConfirmRow(rowRef1);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    auto rowRef2 = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef2.Store == store2);

    StoreManager->PrepareRow(rowRef2);
    PrepareTransaction(transaction.get());

    StoreManager->CommitRow(rowRef2);
    CommitTransaction(transaction.get());

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=1"));
}

TEST_F(TStoreManagerTest, MigrateRowOnPrepare)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false, nullptr);
    EXPECT_EQ(1, transaction->LockedRows().size());

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    const auto& rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    StoreManager->PrepareRow(rowRef);
    PrepareTransaction(transaction.get());
    EXPECT_TRUE(rowRef.Store == store2);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    StoreManager->CommitRow(rowRef);
    CommitTransaction(transaction.get());

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

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false, nullptr);
    EXPECT_EQ(1, transaction->LockedRows().size());

    auto& rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    StoreManager->PrepareRow(rowRef);
    PrepareTransaction(transaction.get());

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    StoreManager->CommitRow(rowRef);
    CommitTransaction(transaction.get());

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=1"));
}

TEST_F(TStoreManagerTest, MigrateRowOnOverwrite)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false, nullptr);
    EXPECT_EQ(1, transaction->LockedRows().size());

    const auto& rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=2").Get(), false, nullptr);
    EXPECT_EQ(1, transaction->LockedRows().size());

    EXPECT_TRUE(rowRef.Store == store2);
    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    StoreManager->PrepareRow(rowRef);
    PrepareTransaction(transaction.get());

    StoreManager->CommitRow(rowRef);
    CommitTransaction(transaction.get());

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    CompareRows(LookupRow(store1, key, LastCommittedTimestamp), Null);
    CompareRows(LookupRow(store2, key, LastCommittedTimestamp), Stroka("key=1;a=2"));
}

TEST_F(TStoreManagerTest, WriteAfterDeleteFailureWithRotation)
{
    auto transaction = StartTransaction();

    StoreManager->DeleteRow(transaction.get(), BuildKey("1").Get(), false, nullptr);

    Rotate();

    ASSERT_ANY_THROW({
        StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=2").Get(), false, nullptr);
    });
}

TEST_F(TStoreManagerTest, WriteWriteConflictWithRotation1)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    StoreManager->WriteRow(transaction1.get(), BuildRow("key=1;a=1").Get(), false, nullptr);

    Rotate();

    ASSERT_ANY_THROW({
        StoreManager->WriteRow(transaction2.get(), BuildRow("key=1;a=1").Get(), false, nullptr);
    });
}

TEST_F(TStoreManagerTest, DISABLED_WriteWriteConflictWithRotation2)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    StoreManager->WriteRow(transaction1.get(), BuildRow("key=1;a=1").Get(), false, nullptr);
    
    EXPECT_EQ(1, transaction1->LockedRows().size());
    const auto& rowRef1 = transaction1->LockedRows()[0];

    StoreManager->PrepareRow(rowRef1);
    PrepareTransaction(transaction1.get());

    StoreManager->CommitRow(rowRef1);
    CommitTransaction(transaction1.get());

    Rotate();

    ASSERT_ANY_THROW({
        StoreManager->WriteRow(transaction2.get(), BuildRow("key=1;a=1").Get(), false, nullptr);
    });
}

TEST_F(TStoreManagerTest, DontMigrateRowOnAbort)
{
    auto store1 = Tablet->GetActiveStore();

    auto transaction = StartTransaction();

    StoreManager->WriteRow(transaction.get(), BuildRow("key=1;a=1").Get(), false, nullptr);
    EXPECT_EQ(1, transaction->LockedRows().size());

    Rotate();
    auto store2 = Tablet->GetActiveStore();

    EXPECT_TRUE(store1 != store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto& rowRef = transaction->LockedRows()[0];
    EXPECT_TRUE(rowRef.Store == store1);

    StoreManager->AbortRow(rowRef);
    AbortTransaction(transaction.get());

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

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

