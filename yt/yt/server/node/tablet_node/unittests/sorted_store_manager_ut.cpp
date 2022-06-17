#include "sorted_store_manager_ut_helpers.h"

namespace NYT::NTabletNode {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TSingleLockStoreManagerTest
    : public TSortedStoreManagerTestBase
{ };

TEST_F(TSingleLockStoreManagerTest, EmptyWriteFailure)
{
    EXPECT_ANY_THROW({
        WriteRow(BuildKey("key=1"));
    });
}

TEST_F(TSingleLockStoreManagerTest, PrelockRow)
{
    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    auto rowRef = WriteRow(transaction.get(), BuildRow("key=1;a=1"), true);

    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(0u, transaction->LockedRows().size());
    EXPECT_EQ(store, rowRef.Store);

    ConfirmRow(transaction.get(), rowRef);
    EXPECT_EQ(1u, transaction->LockedRows().size());
    EXPECT_EQ(rowRef, transaction->LockedRows()[0]);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TSingleLockStoreManagerTest, AbortRow)
{
    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    EXPECT_EQ(1u, transaction->LockedRows().size());
    EXPECT_EQ(1, store->GetLockCount());

    auto rowRef = transaction->LockedRows()[0];

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TSingleLockStoreManagerTest, CommitRow)
{
    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    auto row = BuildRow("key=1;a=1");
    auto rowRef = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(store, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);

    CommitTransaction(transaction.get());
    TWriteRowCommand command{
        .Row = row
    };
    CommitRow(transaction.get(), command, rowRef);

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TSingleLockStoreManagerTest, ConfirmRowWithRotation)
{
    auto store1 = GetActiveStore();
    EXPECT_EQ(0, store1->GetLockCount());

    auto transaction = StartTransaction();

    auto row = BuildRow("key=1;a=42");
    auto rowRef = WriteRow(transaction.get(), row, /*prelock*/ true);
    EXPECT_EQ(0u, transaction->LockedRows().size());
    EXPECT_EQ(store1, rowRef.Store);

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    ConfirmRow(transaction.get(), rowRef);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1u, transaction->LockedRows().size());
    EXPECT_EQ(rowRef, transaction->LockedRows()[0]);
    EXPECT_EQ(store1, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);

    CommitTransaction(transaction.get());
    TWriteRowCommand command{
        .Row = row
    };
    CommitRow(transaction.get(), command, rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    auto expected = "key=1;a=42";
    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), expected));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), expected));
}

TEST_F(TSingleLockStoreManagerTest, PrepareRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    auto row = BuildRow("key=1;a=1");
    auto rowRef = WriteRow(transaction.get(), row, /*prelock*/ false);
    EXPECT_EQ(1u, transaction->LockedRows().size());

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1u, transaction->LockedRows().size());
    EXPECT_EQ(rowRef, transaction->LockedRows()[0]);
    EXPECT_EQ(store1, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);
    EXPECT_EQ(store1, rowRef.Store);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction.get());
    TWriteRowCommand command{
        .Row = row
    };
    CommitRow(transaction.get(), command, rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    auto expected = "key=1;a=1";
    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), expected));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), expected));
}

TEST_F(TSingleLockStoreManagerTest, MigrateRow)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    auto row = BuildRow("key=1;a=1");
    WriteRow(transaction.get(), row, /*prelock*/ false);

    EXPECT_EQ(1u, transaction->LockedRows().size());
    auto& rowRef = transaction->LockedRows()[0];
    EXPECT_EQ(store1, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction.get());
    TWriteRowCommand command{
        .Row = row
    };
    CommitRow(transaction.get(), command, rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
}

TEST_F(TSingleLockStoreManagerTest, WriteWriteConflictWithRotation1)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    WriteRow(transaction1.get(), BuildRow("key=1;a=1"), true);

    RotateStores();

    EXPECT_EQ(TSortedDynamicRowRef(), WriteRow(transaction2.get(), BuildRow("key=1;a=1"), true));
}

TEST_F(TSingleLockStoreManagerTest, WriteWriteConflictWithRotation2)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row = BuildRow("key=1;a=1");
    WriteRow(transaction1.get(), row, /*prelock*/ false);

    EXPECT_EQ(1u, transaction1->LockedRows().size());
    auto rowRef1 = transaction1->LockedRows()[0];

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);

    CommitTransaction(transaction1.get());
    TWriteRowCommand command{
        .Row = row
    };
    CommitRow(transaction1.get(), command, rowRef1);

    RotateStores();

    EXPECT_EQ(TSortedDynamicRowRef(), WriteRow(transaction2.get(), BuildRow("key=1;a=1"), true));
}

TEST_F(TSingleLockStoreManagerTest, WriteWriteConflictWithRotation3)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto store1 = GetActiveStore();

    WriteRow(transaction1.get(), BuildRow("key=1;a=1"), true);

    RotateStores();

    StoreManager_->RemoveStore(store1);

    EXPECT_EQ(TSortedDynamicRowRef(), WriteRow(transaction2.get(), BuildRow("key=1;a=1"), true));
}

TEST_F(TSingleLockStoreManagerTest, AbortRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    EXPECT_EQ(1u, transaction->LockedRows().size());

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto& rowRef = transaction->LockedRows()[0];
    EXPECT_EQ(store1, rowRef.Store);

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), nullptr));
}

TEST_F(TSingleLockStoreManagerTest, LookupRow1)
{
    WriteRow(BuildRow("key=1;a=100", false));
    RotateStores();
    WriteRow(BuildRow("key=1;b=3.14", false));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("1"), AsyncLastCommittedTimestamp), "key=1;a=100;b=3.14"));
}

TEST_F(TSingleLockStoreManagerTest, LookupRow2)
{
    WriteRow(BuildRow("key=1;a=100", false));
    DeleteRow(BuildKey("1"));
    RotateStores();
    WriteRow(BuildRow("key=1;b=3.14", false));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("1"), AsyncLastCommittedTimestamp), "key=1;b=3.14"));
}

TEST_F(TSingleLockStoreManagerTest, LookupRow3)
{
    WriteRow(BuildRow("key=1;a=100", false));
    RotateStores();
    DeleteRow(BuildKey("1"));
    WriteRow(BuildRow("key=1;b=3.14", false));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("1"), AsyncLastCommittedTimestamp), "key=1;b=3.14"));
}

TEST_F(TSingleLockStoreManagerTest, LookupRow4)
{
    WriteRow(BuildRow("key=1;a=100", false));
    RotateStores();
    WriteRow(BuildRow("key=1;b=3.14", false));
    RotateStores();
    WriteRow(BuildRow("key=1;a=200;c=test", false));
    RotateStores();
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("1"), AsyncLastCommittedTimestamp), "key=1;a=200;b=3.14;c=test"));
}

TEST_F(TSingleLockStoreManagerTest, UnlockStoreOnCommit)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();

    auto row = BuildRow("key=1;a=1");
    WriteRow(transaction.get(), row, false);

    EXPECT_EQ(1u, transaction->LockedRows().size());
    auto rowRef = transaction->LockedRows()[0];

    RotateStores();

    EXPECT_TRUE(StoreManager_->IsStoreLocked(store));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);
    CommitTransaction(transaction.get());
    TWriteRowCommand command{
        .Row = row
    };
    CommitRow(transaction.get(), command, rowRef);

    EXPECT_FALSE(StoreManager_->IsStoreLocked(store));
}

TEST_F(TSingleLockStoreManagerTest, UnlockStoreOnAbort)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    EXPECT_EQ(1u, transaction->LockedRows().size());
    auto rowRef = transaction->LockedRows()[0];

    RotateStores();

    EXPECT_TRUE(StoreManager_->IsStoreLocked(store));

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), rowRef);

    EXPECT_FALSE(StoreManager_->IsStoreLocked(store));
}

TEST_F(TSingleLockStoreManagerTest, WriteRotateWrite)
{
    auto store1 = GetActiveStore();
    EXPECT_EQ(0, store1->GetLockCount());

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row1 = BuildRow("key=1;a=1");
    auto rowRef1 = WriteRow(transaction1.get(), row1, /*prelock*/ false);
    EXPECT_EQ(store1, rowRef1.Store);

    auto row2 = BuildRow("key=2;a=2");
    auto rowRef2 = WriteRow(transaction2.get(), row2, /*prelock*/ false);
    EXPECT_EQ(store1, rowRef2.Store);

    EXPECT_EQ(2, store1->GetLockCount());

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), rowRef2);
    CommitTransaction(transaction2.get());
    TWriteRowCommand command2{
        .Row = row2
    };
    CommitRow(transaction2.get(), command2, rowRef2);

    EXPECT_EQ(1, store1->GetLockCount());

    RotateStores();
    auto store2 = GetActiveStore();
    EXPECT_NE(store1, store2);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto transaction3 = StartTransaction();

    auto row3 = BuildRow("key=2;a=3");
    auto rowRef3 = WriteRow(transaction3.get(), row3, /*prelock*/ false);
    EXPECT_EQ(store2, rowRef3.Store);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    PrepareTransaction(transaction3.get());
    PrepareRow(transaction3.get(), rowRef3);
    CommitTransaction(transaction3.get());
    TWriteRowCommand command3{
        .Row = row3
    };
    CommitRow(transaction3.get(), command3, rowRef3);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);
    CommitTransaction(transaction1.get());
    TWriteRowCommand command1{
        .Row = row1
    };
    CommitRow(transaction1.get(), command1, rowRef1);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());
}

////////////////////////////////////////////////////////////////////////////////

class TBlockedWriteTest
    : public TSingleLockStoreManagerTest
    , public ::testing::WithParamInterface<bool>
{ };

TEST_P(TBlockedWriteTest, WriteBlockedWrite)
{
    auto row = BuildRow("key=1;a=1");

    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction1 = StartTransaction();
    auto rowRef1 = WriteRow(transaction1.get(), row, false);
    EXPECT_EQ(store, rowRef1.Store);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);

    auto transaction2 = StartTransaction(transaction1->GetPrepareTimestamp() + 10);

    if (GetParam()) {
        RotateStores();
    }

    auto context = transaction2->CreateWriteContext();
    context.Phase = EWritePhase::Prelock;

    EXPECT_EQ(TSortedDynamicRowRef(), StoreManager_->ModifyRow(row, ERowModificationType::Write, TLockMask(), &context));
    EXPECT_EQ(rowRef1.Row, context.BlockedRow);
    EXPECT_EQ(rowRef1.Store, context.BlockedStore);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_P(TBlockedWriteTest, WriteConflictingWrite)
{
    auto row = BuildRow("key=1;a=1");

    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction1 = StartTransaction();
    auto rowRef1 = WriteRow(transaction1.get(), row, false);
    EXPECT_EQ(store, rowRef1.Store);

    auto transaction2 = StartTransaction(transaction1->GetPrepareTimestamp() + 10);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);

    if (GetParam()) {
        RotateStores();
    }

    auto context = transaction2->CreateWriteContext();
    context.Phase = EWritePhase::Prelock;

    EXPECT_EQ(TSortedDynamicRowRef(), StoreManager_->ModifyRow(row, ERowModificationType::Write, TLockMask(), &context));
    EXPECT_FALSE(context.Error.IsOK());
}

INSTANTIATE_TEST_SUITE_P(BlockedWrite,
    TBlockedWriteTest,
    ::testing::Values(false, true));

////////////////////////////////////////////////////////////////////////////////

class TSingleLockStoreManagerTestWithStringKeys
    : public TSingleLockStoreManagerTest
{
protected:
    TTableSchemaPtr GetSchema() const override
    {
        // NB: Key columns must go first.
        return New<TTableSchema>(std::vector{
            TColumnSchema("key", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("b", EValueType::Double),
            TColumnSchema("c", EValueType::String),
            TColumnSchema("d", EValueType::Any)
        });
    }
};

TEST_F(TSingleLockStoreManagerTestWithStringKeys, StringKey)
{
    WriteRow(BuildRow("key=test;a=100", false));
    WriteRow(BuildRow("key=another_test;a=101", false));
    WriteRow(BuildRow("b=3.14", false));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("test"), AsyncLastCommittedTimestamp), "key=test;a=100"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("another_test"), AsyncLastCommittedTimestamp), "key=another_test;a=101"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("weird_test"), AsyncLastCommittedTimestamp), nullptr));
}

TEST_F(TSingleLockStoreManagerTestWithStringKeys, NullKey)
{
    WriteRow(BuildRow("key=test;a=100", false));
    WriteRow(BuildRow("key=another_test;a=101", false));
    WriteRow(BuildRow("b=3.14", false));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("<type=null>#"), AsyncLastCommittedTimestamp), "b=3.14"));
}

////////////////////////////////////////////////////////////////////////////////

class TSingleLockStoreManagerTestWithCompositeKeys
    : public TSingleLockStoreManagerTest
{
protected:
    TTableSchemaPtr GetSchema() const override
    {
        return New<TTableSchema>(std::vector{
            TColumnSchema("k1", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v", EValueType::Int64)
        });
    }
};

TEST_F(TSingleLockStoreManagerTestWithCompositeKeys, Write)
{
    WriteRow(BuildRow("k1=1;k2=1;v=100", false));
    WriteRow(BuildRow("k1=1;k2=2;v=200", false));
    WriteRow(BuildRow("k1=2;k2=1;v=300", false));
    WriteRow(BuildRow("k1=2;k2=2;v=400", false));

    WriteRow(BuildRow("     k2=2;v=500", false));
    WriteRow(BuildRow("k1=2;     v=600", false));
    WriteRow(BuildRow("          v=700", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("1;1"), AsyncLastCommittedTimestamp), "k1=1;k2=1;v=100"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("1;2"), AsyncLastCommittedTimestamp), "k1=1;k2=2;v=200"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("2;1"), AsyncLastCommittedTimestamp), "k1=2;k2=1;v=300"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("2;2"), AsyncLastCommittedTimestamp), "k1=2;k2=2;v=400"));

    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("<type=null>#;           2"), AsyncLastCommittedTimestamp), "     k2=2;v=500"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("2;           <type=null>#"), AsyncLastCommittedTimestamp), "k1=2;     v=600"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(BuildKey("<type=null>#;<type=null>#"), AsyncLastCommittedTimestamp), "          v=700"));
}

////////////////////////////////////////////////////////////////////////////////

class TMultiLockStoreManagerTest
    : public TSortedStoreManagerTestBase
{
protected:
    TTableSchemaPtr GetSchema() const override
    {
        // NB: Key columns must go first.
        return New<TTableSchema>(std::vector{
            TColumnSchema("key", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64).SetLock(TString("l1")),
            TColumnSchema("b", EValueType::Double).SetLock(TString("l2")),
            TColumnSchema("c", EValueType::String)
        });
    }
};

TEST_F(TMultiLockStoreManagerTest, WriteTakesPrimaryLock)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;c=text", false), false).Row;
    EXPECT_EQ(transaction_, GetLock(row, 0).WriteTransaction);
    EXPECT_EQ(nullptr, GetLock(row, 1).WriteTransaction);
    EXPECT_EQ(nullptr, GetLock(row, 2).WriteTransaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, WriteTakesSecondaryLocks1)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;a=1", false), false).Row;
    EXPECT_EQ(nullptr, GetLock(row, 0).WriteTransaction);
    EXPECT_EQ(transaction_, GetLock(row, 1).WriteTransaction);
    EXPECT_EQ(nullptr, GetLock(row, 2).WriteTransaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, WriteTakesSecondaryLocks2)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;b=3.14", false), false).Row;
    EXPECT_EQ(nullptr, GetLock(row, 0).WriteTransaction);
    EXPECT_EQ(nullptr, GetLock(row, 1).WriteTransaction);
    EXPECT_EQ(transaction_, GetLock(row, 2).WriteTransaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, WriteTakesSecondaryLocks3)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;a=1;b=3.14", false), false).Row;
    EXPECT_EQ(nullptr, GetLock(row, 0).WriteTransaction);
    EXPECT_EQ(transaction_, GetLock(row, 1).WriteTransaction);
    EXPECT_EQ(transaction_, GetLock(row, 2).WriteTransaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, DeleteTakesPrimaryLock)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = DeleteRow(transaction_, BuildKey("1"), false).Row;
    EXPECT_EQ(transaction_, GetLock(row, 0).WriteTransaction);
    EXPECT_EQ(nullptr, GetLock(row, 1).WriteTransaction);
    EXPECT_EQ(nullptr, GetLock(row, 2).WriteTransaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, MigrateRow1)
{
    auto key = BuildKey("1");

    auto store1 = GetActiveStore();

    auto transaction1 = StartTransaction();

    auto row1 = BuildRow("key=1;a=1", false);
    WriteRow(transaction1.get(), row1, /*prelock*/ false);
    EXPECT_EQ(1u, transaction1->LockedRows().size());
    auto& rowRef1 = transaction1->LockedRows()[0];
    EXPECT_EQ(store1, rowRef1.Store);

    auto transaction2 = StartTransaction();

    auto row2 = BuildRow("key=1;b=3.14", false);
    WriteRow(transaction2.get(), row2, /*prelock*/ false);
    EXPECT_EQ(1u, transaction2->LockedRows().size());
    auto& rowRef2 = transaction2->LockedRows()[0];
    EXPECT_EQ(store1, rowRef1.Store);

    EXPECT_EQ(rowRef1.Row, rowRef2.Row);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), rowRef2);

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(2, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction1.get());
    TWriteRowCommand command1{
        .Row = row1
    };
    CommitRow(transaction1.get(), command1, rowRef1);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;a=1"));

    CommitTransaction(transaction2.get());
    TWriteRowCommand command2{
        .Row = row2
    };
    CommitRow(transaction2.get(), command2, rowRef2);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;a=1;b=3.14"));
}

TEST_F(TMultiLockStoreManagerTest, MigrateRow2)
{
    auto key = BuildKey("1");

    auto store1 = GetActiveStore();

    auto transaction1 = StartTransaction();

    auto row1 = BuildRow("key=1;a=1", false);
    WriteRow(transaction1.get(), row1, /*prelock*/ false);
    EXPECT_EQ(1u, transaction1->LockedRows().size());
    auto& rowRef1 = transaction1->LockedRows()[0];
    EXPECT_EQ(store1, rowRef1.Store);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto transaction2 = StartTransaction();

    auto row2 = BuildRow("key=1;b=3.14", false);
    WriteRow(transaction2.get(), row2, /*prelock*/ false);
    EXPECT_EQ(1u, transaction2->LockedRows().size());
    auto& rowRef2 = transaction2->LockedRows()[0];
    EXPECT_EQ(store1, rowRef1.Store);

    EXPECT_NE(rowRef1.Row, rowRef2.Row);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), rowRef2);

    CommitTransaction(transaction2.get());
    TWriteRowCommand command2{
        .Row = row2
    };
    CommitRow(transaction2.get(), command2, rowRef2);

    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;b=3.14"));

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction1.get());

    TWriteRowCommand command1{
        .Row = row1
    };
    CommitRow(transaction1.get(), command1, rowRef1);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;a=1;b=3.14"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

