#include "sorted_dynamic_store_ut_helpers.h"

#include <yt/server/tablet_node/lookup.h>
#include <yt/server/tablet_node/sorted_store_manager.h>

#include <yt/ytlib/tablet_client/wire_protocol.h>
#include <yt/ytlib/tablet_client/wire_protocol.pb.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NObjectClient;
using namespace NTabletClient::NProto;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreManagerTestBase
    : public TStoreManagerTestBase<TSortedDynamicStoreTestBase>
{
protected:
    virtual IStoreManagerPtr CreateStoreManager(TTablet* tablet) override
    {
        YCHECK(!StoreManager_);
        StoreManager_ = New<TSortedStoreManager>(
            New<TTabletManagerConfig>(),
            tablet,
            this);
        return StoreManager_;
    }

    virtual IStoreManagerPtr GetStoreManager() override
    {
        return StoreManager_;
    }

    TSortedDynamicRowRef WriteRow(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;
        return StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, &context);
    }

    void WriteRow(const TUnversionedOwningRow& row, bool prelock = false)
    {
        auto transaction = StartTransaction();

        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction.get();
        auto rowRef = StoreManager_->ModifyRow(row, NApi::ERowModificationType::Write, &context);

        if (prelock) {
            EXPECT_EQ(1, transaction->PrelockedRows().size());
            EXPECT_EQ(rowRef, transaction->PrelockedRows().front());
        } else {
            EXPECT_EQ(1, transaction->LockedRows().size());
            EXPECT_EQ(rowRef, transaction->LockedRows().front());
        }

        PrepareTransaction(transaction.get());
        StoreManager_->PrepareRow(transaction.get(), rowRef);

        CommitTransaction(transaction.get());
        StoreManager_->CommitRow(transaction.get(), rowRef);
    }

    TSortedDynamicRowRef DeleteRow(
        TTransaction* transaction,
        TUnversionedRow row,
        bool prelock)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;
        return StoreManager_->ModifyRow(row, ERowModificationType::Delete, &context);
    }

    void DeleteRow(const TOwningKey& key)
    {
        auto transaction = StartTransaction();

        DeleteRow(transaction.get(), key, false);

        EXPECT_EQ(1, transaction->LockedRows().size());
        auto rowRef = transaction->LockedRows()[0];

        PrepareTransaction(transaction.get());
        StoreManager_->PrepareRow(transaction.get(), rowRef);

        CommitTransaction(transaction.get());
        StoreManager_->CommitRow(transaction.get(), rowRef);
    }

    void PrepareRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->PrepareRow(transaction, rowRef);
    }

    void CommitRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->CommitRow(transaction, rowRef);
    }

    void AbortRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->AbortRow(transaction, rowRef);
    }

    void ConfirmRow(TTransaction* transaction, const TSortedDynamicRowRef& rowRef)
    {
        StoreManager_->ConfirmRow(transaction, rowRef);
    }

    using TSortedDynamicStoreTestBase::LookupRow;
    
    TUnversionedOwningRow LookupRow(const TOwningKey& key, TTimestamp timestamp)
    {
        TSharedRef request;
        {
            TReqLookupRows req;
            std::vector<TUnversionedRow> keys(1, key);

            TWireProtocolWriter writer;
            writer.WriteMessage(req);
            writer.WriteSchemafulRowset(keys);

            struct TMergedTag { };
            request = MergeRefsToRef<TMergedTag>(writer.Finish());
        }

        TSharedRef response;
        {
            TWireProtocolReader reader(request);
            TWireProtocolWriter writer;
            LookupRows(
                Tablet_->BuildSnapshot(nullptr),
                timestamp,
                "ut",
                TWorkloadDescriptor(),
                TReadSessionId(),
                &reader,
                &writer);
            struct TMergedTag { };
            response = MergeRefsToRef<TMergedTag>(writer.Finish());
        }

        {
            TWireProtocolReader reader(response);
            auto schemaData = TWireProtocolReader::GetSchemaData(Tablet_->PhysicalSchema(), TColumnFilter());
            auto row = reader.ReadSchemafulRow(schemaData, false);
            return TUnversionedOwningRow(row);
        }
    }
    
    TSortedDynamicStorePtr GetActiveStore()
    {
        return Tablet_->GetActiveStore()->AsSortedDynamic();
    }


    TSortedStoreManagerPtr StoreManager_;

};

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
    EXPECT_EQ(0, transaction->LockedRows().size());
    EXPECT_EQ(store, rowRef.Store);

    ConfirmRow(transaction.get(), rowRef);
    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_EQ(rowRef, transaction->LockedRows()[0]);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TSingleLockStoreManagerTest, AbortRow)
{
    auto store = GetActiveStore();
    EXPECT_EQ(0, store->GetLockCount());

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    EXPECT_EQ(1, transaction->LockedRows().size());
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

    auto rowRef = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    EXPECT_EQ(1, store->GetLockCount());
    EXPECT_EQ(store, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store->GetLockCount());
}

TEST_F(TSingleLockStoreManagerTest, ConfirmRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    auto rowRef = WriteRow(transaction.get(), BuildRow("key=1;a=1"), true);
    EXPECT_EQ(0, transaction->LockedRows().size());
    EXPECT_EQ(store1, rowRef.Store);

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    ConfirmRow(transaction.get(), rowRef);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_EQ(rowRef, transaction->LockedRows()[0]);
    EXPECT_EQ(store1, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    auto row = "key=1;a=1";
    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), row));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), row));
}

TEST_F(TSingleLockStoreManagerTest, PrepareRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    auto rowRef = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    EXPECT_EQ(1, transaction->LockedRows().size());

    RotateStores();
    auto store2 = GetActiveStore();

    EXPECT_NE(store1, store2);
    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_EQ(1, transaction->LockedRows().size());
    EXPECT_EQ(rowRef, transaction->LockedRows()[0]);
    EXPECT_EQ(store1, rowRef.Store);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);
    EXPECT_EQ(store1, rowRef.Store);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    auto row = "key=1;a=1";
    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), row));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), row));
}

TEST_F(TSingleLockStoreManagerTest, MigrateRow)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    EXPECT_EQ(1, transaction->LockedRows().size());
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
    CommitRow(transaction.get(), rowRef);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
}

TEST_F(TSingleLockStoreManagerTest, WriteSameRowWithRotation)
{
    auto store1 = GetActiveStore();

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), true);

    RotateStores();

    EXPECT_EQ(TSortedDynamicRowRef(), WriteRow(transaction.get(), BuildRow("key=1;a=2"), true));
}

TEST_F(TSingleLockStoreManagerTest, DeleteSameRowWithRotation)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    DeleteRow(transaction.get(), key, true);

    RotateStores();

    EXPECT_EQ(TSortedDynamicRowRef(), DeleteRow(transaction.get(), key, true));
}

TEST_F(TSingleLockStoreManagerTest, WriteAfterDeleteFailureWithRotation)
{
    auto transaction = StartTransaction();

    DeleteRow(transaction.get(), BuildKey("1"), true);

    RotateStores();

    EXPECT_EQ(TSortedDynamicRowRef(), WriteRow(transaction.get(), BuildRow("key=1;a=2"), true));
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

    WriteRow(transaction1.get(), BuildRow("key=1;a=1"), false);
    
    EXPECT_EQ(1, transaction1->LockedRows().size());
    auto rowRef1 = transaction1->LockedRows()[0];

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), rowRef1);

    CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), rowRef1);

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
    EXPECT_EQ(1, transaction->LockedRows().size());

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

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    EXPECT_EQ(1, transaction->LockedRows().size());
    auto rowRef = transaction->LockedRows()[0];

    RotateStores();

    EXPECT_TRUE(StoreManager_->IsStoreLocked(store));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), rowRef);
    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), rowRef);

    EXPECT_FALSE(StoreManager_->IsStoreLocked(store));
}

TEST_F(TSingleLockStoreManagerTest, UnlockStoreOnAbort)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    EXPECT_EQ(1, transaction->LockedRows().size());
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

    auto rowRef1 = WriteRow(transaction1.get(), BuildRow("key=1;a=1"), false);
    EXPECT_EQ(store1, rowRef1.Store);

    auto rowRef2 = WriteRow(transaction2.get(), BuildRow("key=2;a=2"), false);
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

    auto rowRef3 = WriteRow(transaction3.get(), BuildRow("key=2;a=3"), false);
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

TEST_F(TSingleLockStoreManagerTest, WriteBlockedWrite)
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

    TWriteContext context;
    context.Phase = EWritePhase::Prelock;
    context.Transaction = transaction2.get();
    EXPECT_EQ(TSortedDynamicRowRef(), StoreManager_->ModifyRow(row, ERowModificationType::Write, &context));
    EXPECT_EQ(rowRef1.Row, context.BlockedRow);
    EXPECT_EQ(rowRef1.Store, context.BlockedStore);
    EXPECT_EQ(1, store->GetLockCount());
}

////////////////////////////////////////////////////////////////////////////////

class TSingleLockStoreManagerTestWithStringKeys
    : public TSingleLockStoreManagerTest
{
protected:
    virtual TTableSchema GetSchema() const
    {
        // NB: Key columns must go first.
        TTableSchema schema({
            TColumnSchema("key", EValueType::String).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("b", EValueType::Double),
            TColumnSchema("c", EValueType::String),
            TColumnSchema("d", EValueType::Any)
        });
        return schema;
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
    virtual TTableSchema GetSchema() const
    {
        // NB: Key columns must go first.
        TTableSchema schema({
            TColumnSchema("k1", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("k2", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("v", EValueType::Int64)
        });
        return schema;
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
    virtual TTableSchema GetSchema() const
    {
        // NB: Key columns must go first.
        TTableSchema schema({
            TColumnSchema("key", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64).SetLock(TString("l1")),
            TColumnSchema("b", EValueType::Double).SetLock(TString("l2")),
            TColumnSchema("c", EValueType::String)
        });
        return schema;
    }

};

TEST_F(TMultiLockStoreManagerTest, WriteTakesPrimaryLock)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;c=text", false), false).Row;
    EXPECT_EQ(transaction_, GetLock(row, 0).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 1).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 2).Transaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, WriteTakesSecondaryLocks1)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;a=1", false), false).Row;
    EXPECT_EQ(nullptr, GetLock(row, 0).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 1).Transaction);
    EXPECT_EQ(nullptr, GetLock(row, 2).Transaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, WriteTakesSecondaryLocks2)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;b=3.14", false), false).Row;
    EXPECT_EQ(nullptr, GetLock(row, 0).Transaction);
    EXPECT_EQ(nullptr, GetLock(row, 1).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 2).Transaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, WriteTakesSecondaryLocks3)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = WriteRow(transaction_, BuildRow("key=1;a=1;b=3.14", false), false).Row;
    EXPECT_EQ(nullptr, GetLock(row, 0).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 1).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 2).Transaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, DeleteTakesPrimaryLock)
{
    auto store = GetActiveStore();
    auto transaction = StartTransaction();
    auto* transaction_ = transaction.get();
    auto row = DeleteRow(transaction_, BuildKey("1"), false).Row;
    EXPECT_EQ(transaction_, GetLock(row, 0).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 1).Transaction);
    EXPECT_EQ(transaction_, GetLock(row, 2).Transaction);
    EXPECT_EQ(1, store->GetLockCount());
}

TEST_F(TMultiLockStoreManagerTest, MigrateRow1)
{
    auto key = BuildKey("1");

    auto store1 = GetActiveStore();

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), false);
    EXPECT_EQ(1, transaction1->LockedRows().size());
    auto& rowRef1 = transaction1->LockedRows()[0];
    EXPECT_EQ(store1, rowRef1.Store);

    auto transaction2 = StartTransaction();
    WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), false);
    EXPECT_EQ(1, transaction2->LockedRows().size());
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
    CommitRow(transaction1.get(), rowRef1);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;a=1"));

    CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), rowRef2);

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
    WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), false);
    EXPECT_EQ(1, transaction1->LockedRows().size());
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
    WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), false);
    EXPECT_EQ(1, transaction2->LockedRows().size());
    auto& rowRef2 = transaction2->LockedRows()[0];
    EXPECT_EQ(store1, rowRef1.Store);

    EXPECT_NE(rowRef1.Row, rowRef2.Row);

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(1, store2->GetLockCount());

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), rowRef2);

    CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), rowRef2);

    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;b=3.14"));

    EXPECT_EQ(1, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), rowRef1);

    EXPECT_EQ(0, store1->GetLockCount());
    EXPECT_EQ(0, store2->GetLockCount());

    EXPECT_TRUE(AreRowsEqual(LookupRow(store1, key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(store2, key, AsyncLastCommittedTimestamp), "key=1;a=1;b=3.14"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

