#include "stdafx.h"
#include "memory_store_ut.h"

#include <yt/core/actions/invoker_util.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TSingleLockDynamicMemoryStoreTest
    : public TMemoryStoreTestBase
{
protected:
    virtual void SetUp() override
    {
        TMemoryStoreTestBase::SetUp();

        auto config = New<TTabletManagerConfig>();
        config->MaxBlockedRowWaitTime = TDuration::MilliSeconds(100);

        Store_ = New<TDynamicMemoryStore>(
            config,
            TTabletId(),
            Tablet_.get());
    }


    void ConfirmRow(TTransaction* transaction, TDynamicRow row)
    {
        Store_->ConfirmRow(transaction, row);
    }

    void PrepareRow(TTransaction* transaction, TDynamicRow row)
    {
        Store_->PrepareRow(transaction, row);
    }

    void CommitRow(TTransaction* transaction, TDynamicRow row)
    {
        Store_->CommitRow(transaction, row);
    }

    void AbortRow(TTransaction* transaction, TDynamicRow row)
    {
        Store_->AbortRow(transaction, row);
    }

    TDynamicRow WriteRow(
        TTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock,
        ui32 lockMask = TDynamicRow::PrimaryLockMask)
    {
        return Store_->WriteRow(transaction, row.Get(), prelock, lockMask);
    }

    TTimestamp WriteRow(
        const TUnversionedOwningRow& row,
        ui32 lockMask = TDynamicRow::PrimaryLockMask)
    {
        auto transaction = StartTransaction();
        auto dynamicRow = WriteRow(transaction.get(), row, false, lockMask);
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), dynamicRow);
        auto ts = CommitTransaction(transaction.get());
        CommitRow(transaction.get(), dynamicRow);
        return ts;
    }

    TDynamicRow DeleteRow(
        TTransaction* transaction,
        const TOwningKey& key,
        bool prelock)
    {
        return Store_->DeleteRow(transaction, key.Get(), prelock);
    }

    TTimestamp DeleteRow(const TOwningKey& key)
    {
        auto transaction = StartTransaction();
        auto row = DeleteRow(transaction.get(), key, false);
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), row);
        auto ts = CommitTransaction(transaction.get());
        CommitRow(transaction.get(), row);
        return ts;
    }


    TUnversionedOwningRow LookupRow(const TOwningKey& key, TTimestamp timestamp)
    {
        return TMemoryStoreTestBase::LookupRow(Store_, key, timestamp);
    }


    TDynamicMemoryStorePtr Store_;

};

///////////////////////////////////////////////////////////////////////////////

TEST_F(TSingleLockDynamicMemoryStoreTest, Empty)
{
    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 0), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Null));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, PrelockWriteAndCommit)
{
    auto transaction = StartTransaction();

    auto key = BuildKey("1");

    Stroka rowString("key=1;a=1");

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Null));

    auto row = WriteRow(transaction.get(), BuildRow(rowString), true);
    ASSERT_FALSE(row.GetDeleteLockFlag());
    const auto& lock = GetLock(row);
    ASSERT_EQ(transaction.get(), lock.Transaction);
    ASSERT_TRUE(transaction->LockedRows().empty());

    ConfirmRow(transaction.get(), row);
    ASSERT_EQ(1, transaction->LockedRows().size());
    ASSERT_TRUE(transaction->LockedRows()[0].Row == row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Null));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    auto ts = CommitTransaction(transaction.get());
    CommitRow(transaction.get(), row);

    ASSERT_FALSE(row.GetDeleteLockFlag());
    ASSERT_EQ(nullptr, lock.Transaction);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts - 1), Null));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, PrelockDeleteAndCommit)
{
    auto key = BuildKey("1");
    Stroka rowString("key=1;a=1");

    auto ts1 = WriteRow(BuildRow(rowString, false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), rowString));

    auto transaction = StartTransaction();

    auto row = DeleteRow(transaction.get(), key, true);
    ASSERT_TRUE(row.GetDeleteLockFlag());
    const auto& lock = GetLock(row);
    ASSERT_EQ(transaction.get(), lock.Transaction);
    ASSERT_TRUE(transaction->LockedRows().empty());

    ConfirmRow(transaction.get(), row);
    ASSERT_EQ(1, transaction->LockedRows().size());
    ASSERT_TRUE(transaction->LockedRows()[0].Row == row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), rowString));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    auto ts2 = CommitTransaction(transaction.get());
    CommitRow(transaction.get(), row);

    ASSERT_FALSE(row.GetDeleteLockFlag());
    ASSERT_EQ(nullptr, lock.Transaction);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Null));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, PrelockManyWritesAndCommit)
{
    auto key = BuildKey("1");

    std::vector<TTimestamp> timestamps;

    for (int i = 0; i < 100; ++i) {
        auto transaction = StartTransaction();

        if (i == 0) {
            EXPECT_TRUE(AreRowsEqual(LookupRow(key, transaction->GetStartTimestamp()), Null));
        } else {
            EXPECT_TRUE(AreRowsEqual(LookupRow(key, transaction->GetStartTimestamp()), "key=1;a=" + ToString(i - 1)));
        }

        auto row = WriteRow(transaction.get(), BuildRow("key=1;a=" + ToString(i)), false);

        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), row);

        auto ts = CommitTransaction(transaction.get());
        CommitRow(transaction.get(), row);

        timestamps.push_back(ts);
    }


    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), Stroka("key=1;a=99")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Stroka("key=1;a=99")));

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, timestamps[i]), Stroka("key=1;a=" + ToString(i))));
    }
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteSameRow)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;b=3.14"), false);
    ASSERT_ANY_THROW({
       WriteRow(transaction.get(), BuildRow("key=1;b=2.71"), false);
    });
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteAndAbort)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;b=3.14"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), row);

    const auto& lock = GetLock(row);
    ASSERT_EQ(nullptr, lock.Transaction);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, Delete)
{
    auto key = BuildKey("1");
    auto ts = DeleteRow(key);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Null));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteDelete)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;c=value"));

    auto transaction2 = StartTransaction();

    auto row = DeleteRow(transaction2.get(), key, false);
    EXPECT_EQ(ts1, GetLock(row).LastCommitTimestamp);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Stroka("key=1;c=value")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;c=value")));

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;c=value")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Null));
    EXPECT_EQ(ts2, GetLock(row).LastCommitTimestamp);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteWrite)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;a=1"));

    auto transaction2 = StartTransaction();

    auto row = WriteRow(transaction2.get(), BuildRow("key=1;b=3.14"), false);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Stroka("key=1;a=1")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1")));

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Stroka("key=1;b=3.14")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Stroka("key=1;b=3.14")));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, DeleteSameRow)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    DeleteRow(transaction.get(), key, false);
    ASSERT_ANY_THROW({
        DeleteRow(transaction.get(), key, false);
    });
}

TEST_F(TSingleLockDynamicMemoryStoreTest, Update1)
{
    auto key = BuildKey("1");
    
    auto ts = WriteRow(BuildRow("key=1", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), Stroka("key=1")));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, Update2)
{
    auto key = BuildKey("1");
    
    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts2 = WriteRow(BuildRow("key=1;b=3.0", false));
    auto ts3 = WriteRow(BuildRow("key=1;c=test", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Stroka("key=1;a=1;b=3.0")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), Stroka("key=1;a=1;b=3.0;c=test")));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, Update3)
{
    auto key = BuildKey("1");
    
    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts2 = WriteRow(BuildRow("key=1;a=2", false));
    auto ts3 = WriteRow(BuildRow("key=1;a=3", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Stroka("key=1;a=2")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), Stroka("key=1;a=3")));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, UpdateDelete1)
{
    auto key = BuildKey("1");
    
    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts2 = DeleteRow(key);
    auto ts3 = WriteRow(BuildRow("key=1;b=2.0", false));
    auto ts4 = DeleteRow(key);
    auto ts5 = WriteRow(BuildRow("key=1;c=test", false));
    auto ts6 = DeleteRow(key);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), Stroka("key=1;b=2.0")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts4), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts5), Stroka("key=1;c=test")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts6), Null));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, UpdateDelete2)
{
    auto key = BuildKey("1");
    
    auto ts1 = DeleteRow(key);
    auto ts2 = DeleteRow(key);
    auto ts3 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts4 = DeleteRow(key);
    auto ts5 = DeleteRow(key);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), Stroka("key=1;a=1")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts4), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts5), Null));
}

TEST_F(TSingleLockDynamicMemoryStoreTest, DeleteAfterWriteFailure1)
{
    auto transaction = StartTransaction();
    WriteRow(transaction.get(), BuildRow("key=1"), true);
    ASSERT_ANY_THROW({
        DeleteRow(transaction.get(), BuildKey("1"), true);
    });
}

TEST_F(TSingleLockDynamicMemoryStoreTest, DeleteAfterWriteFailure2)
{
    WriteRow(BuildRow("key=1"));

    {
        auto transaction = StartTransaction();
        WriteRow(transaction.get(), BuildRow("key=1"), true);
        ASSERT_ANY_THROW({
            DeleteRow(transaction.get(), BuildKey("1"), true);
        });
    }
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteAfterDeleteFailure1)
{
    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), BuildKey("1"), true);
    ASSERT_ANY_THROW({
        WriteRow(transaction.get(), BuildRow("key=1"), true);
    });
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteAfterDeleteFailure2)
{
    WriteRow(BuildRow("key=1"));

    {
        auto transaction = StartTransaction();
        DeleteRow(transaction.get(), BuildKey("1"), true);
        ASSERT_ANY_THROW({
            WriteRow(transaction.get(), BuildRow("key=1"), true);
        });
    }
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;c=test1"), true);
    ASSERT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;c=test2"), true);
    });
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteWriteConflict2)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row = WriteRow(transaction1.get(), BuildRow("key=1;a=1"), true);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row);

    CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), row);

    ASSERT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;a=2"), true);
    });
}

TEST_F(TSingleLockDynamicMemoryStoreTest, ReadNotBlocked)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    
    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SubscribeRowBlocked(BIND([&] (TDynamicRow /*row*/, int /*lockIndex*/) {
        blocked = true;
    }));

    // Not blocked.
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), Null));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, transaction->GetPrepareTimestamp()), Null));

    EXPECT_FALSE(blocked);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, ReadBlockedAbort)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    
    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    
    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SubscribeRowBlocked(BIND([&] (TDynamicRow blockedRow, int lockIndex) {
        EXPECT_EQ(TDynamicRow::PrimaryLockIndex, lockIndex);
        EXPECT_EQ(blockedRow, row);
        AbortTransaction(transaction.get());
        AbortRow(transaction.get(), row);
        blocked = true;
    }));

    // Blocked, old value is read.
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), Null));
    EXPECT_TRUE(blocked);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, ReadBlockedCommit)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    
    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);
    
    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SubscribeRowBlocked(BIND([&] (TDynamicRow blockedRow, int lockIndex) {
        EXPECT_EQ(TDynamicRow::PrimaryLockIndex, lockIndex);
        EXPECT_EQ(blockedRow, row);
        CommitTransaction(transaction.get());
        CommitRow(transaction.get(), row);
        blocked = true;
    }));

    // Blocked, new value is read.
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), Stroka("key=1;a=1")));
    EXPECT_TRUE(blocked);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, ReadBlockedTimeout)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SubscribeRowBlocked(BIND([&] (TDynamicRow blockedRow, int lockIndex) {
        blocked = true;
        Sleep(TDuration::MilliSeconds(10));
    }));

    // Blocked, timeout.
    EXPECT_ANY_THROW({
        LookupRow(key, SyncLastCommittedTimestamp);
    });
    EXPECT_TRUE(blocked);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteNotBlocked)
{
    auto inputRow = BuildRow("key=1;a=1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row = WriteRow(transaction1.get(), inputRow, false);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row);

    bool blocked = false;
    Store_->SubscribeRowBlocked(BIND([&] (TDynamicRow /*blockedRow*/, int /*lockIndex*/) {
        blocked = true;
    }));

    // Not blocked, write conflicted.
    EXPECT_ANY_THROW({
        WriteRow(transaction2.get(), inputRow, true);
    });
    EXPECT_FALSE(blocked);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, WriteBlocked)
{
    auto inputRow = BuildRow("key=1;a=1");

    auto transaction1 = StartTransaction();

    auto row = WriteRow(transaction1.get(), inputRow, false);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row);

    auto transaction2 = StartTransaction();

    // Blocked, no value is written.
    EXPECT_THROW({
        WriteRow(transaction2.get(), inputRow, true);
    }, TRowBlockedException);
}

TEST_F(TSingleLockDynamicMemoryStoreTest, ArbitraryKeyLength)
{
    WriteRow(BuildRow("key=1;a=1"));

    auto reader = Store_->CreateReader(
        BuildKey("1"),
        BuildKey("1;<type=max>#"),
        AsyncLastCommittedTimestamp,
        TColumnFilter());

    EXPECT_TRUE(reader->Open().Get().IsOK());

    std::vector<TVersionedRow> rows;
    rows.reserve(10);

    EXPECT_TRUE(reader->Read(&rows));
    EXPECT_EQ(1, rows.size());

    EXPECT_FALSE(reader->Read(&rows));
}

///////////////////////////////////////////////////////////////////////////////

class TMultiLockDynamicMemoryStoreTest
    : public TSingleLockDynamicMemoryStoreTest
{
protected:
    virtual TTableSchema GetSchema() const
    {
        // NB: Key columns must go first.
        TTableSchema schema;
        schema.Columns().push_back(TColumnSchema("key", EValueType::Int64));
        schema.Columns().push_back(TColumnSchema("a", EValueType::Int64, Stroka("l1")));
        schema.Columns().push_back(TColumnSchema("b", EValueType::Double, Stroka("l2")));
        schema.Columns().push_back(TColumnSchema("c", EValueType::String));
        return schema;
    }

    static const ui32 LockMask1 = 1 << 1;
    static const ui32 LockMask2 = 1 << 2;

};

TEST_F(TMultiLockDynamicMemoryStoreTest, ConcurrentWrites1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto row = WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), true, LockMask1);

    auto transaction2 = StartTransaction();
    EXPECT_EQ(row, WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), true, LockMask2));

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row);

    auto ts1 = CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1")));
    EXPECT_EQ(MinTimestamp, GetLock(row).LastCommitTimestamp);
    EXPECT_EQ(ts1, GetLock(row, 1).LastCommitTimestamp);

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Stroka("key=1;a=1;b=3.14")));
    EXPECT_EQ(MinTimestamp, GetLock(row).LastCommitTimestamp);
    EXPECT_EQ(ts1, GetLock(row, 1).LastCommitTimestamp);
    EXPECT_EQ(ts2, GetLock(row, 2).LastCommitTimestamp);
}

TEST_F(TMultiLockDynamicMemoryStoreTest, ConcurrentWrites2)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();

    auto transaction2 = StartTransaction();
    auto row = WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), true, LockMask2);

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Stroka("key=1;b=3.14")));
    EXPECT_EQ(MinTimestamp, GetLock(row).LastCommitTimestamp);
    EXPECT_EQ(MinTimestamp, GetLock(row, 1).LastCommitTimestamp);
    EXPECT_EQ(ts2, GetLock(row, 2).LastCommitTimestamp);

    EXPECT_EQ(row, WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), true, LockMask1));

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row);

    auto ts1 = CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), Stroka("key=1;a=1;b=3.14")));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), Stroka("key=1;b=3.14")));
    EXPECT_EQ(MinTimestamp, GetLock(row).LastCommitTimestamp);
    EXPECT_EQ(ts1, GetLock(row, 1).LastCommitTimestamp);
    EXPECT_EQ(ts2, GetLock(row, 2).LastCommitTimestamp);
}

TEST_F(TMultiLockDynamicMemoryStoreTest, WriteWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), true, LockMask1);

    auto transaction2 = StartTransaction();
    EXPECT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;a=2", false), true, LockMask1);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, WriteWriteConflict2)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;a=1;b=3.14", false), true, LockMask1|LockMask2);

    auto transaction2 = StartTransaction();
    EXPECT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;a=2", false), true, LockMask1);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, WriteWriteConflict3)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;c=test", false), true, TDynamicRow::PrimaryLockMask);

    auto transaction2 = StartTransaction();
    EXPECT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;a=1", false), true, LockMask1);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, WriteWriteConflict4)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    WriteRow(BuildRow("key=1;a=1;b=3.14", false));

    EXPECT_ANY_THROW({
        WriteRow(transaction.get(), BuildRow("key=1;a=1", false), true, LockMask1);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, WriteDeleteConflict1)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;a=1", false), true, LockMask1);

    EXPECT_ANY_THROW({
        DeleteRow(key);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, WriteDeleteConflict2)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    WriteRow(BuildRow("key=1;a=1", false), LockMask1);

    EXPECT_ANY_THROW({
        DeleteRow(transaction.get(), key, true);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, DeleteWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    DeleteRow(key);

    EXPECT_ANY_THROW({
        WriteRow(transaction.get(), BuildRow("key=1;a=1", false), true, LockMask1);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, DeleteWriteConflict2)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    DeleteRow(transaction.get(), key, true);

    EXPECT_ANY_THROW({
        WriteRow(BuildRow("key=1;a=1", false), LockMask1);
    });
}

TEST_F(TMultiLockDynamicMemoryStoreTest, WriteNotBlocked)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row1 = WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), false, LockMask1);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row1);

    bool blocked = false;
    Store_->SubscribeRowBlocked(BIND([&] (TDynamicRow /*blockedRow*/, int /*lockIndex*/) {
        blocked = true;
    }));

    // Not blocked, not conflicted.
    auto row2 = WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), true, LockMask2);
    EXPECT_EQ(row1, row2);
    EXPECT_FALSE(blocked);
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

