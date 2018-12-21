#include "sorted_dynamic_store_ut_helpers.h"

namespace NYT::NTabletNode {
namespace {

using namespace NApi;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

class TSingleLockSortedDynamicStoreTest
    : public TSortedDynamicStoreTestBase
{
protected:
    virtual void SetUp() override
    {
        TSortedDynamicStoreTestBase::SetUp();
        CreateDynamicStore();
    }


    void ConfirmRow(TTransaction* transaction, TSortedDynamicRow row)
    {
        transaction->LockedRows().push_back(TSortedDynamicRowRef(Store_.Get(), nullptr, row));
    }

    void PrepareRow(TTransaction* transaction, TSortedDynamicRow row)
    {
        Store_->PrepareRow(transaction, row);
    }

    void CommitRow(TTransaction* transaction, TSortedDynamicRow row)
    {
        Store_->CommitRow(transaction, row);
    }

    void AbortRow(TTransaction* transaction, TSortedDynamicRow row)
    {
        Store_->AbortRow(transaction, row);
    }


    TSortedDynamicRow WriteRow(
        TTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock,
        ui32 lockMask = TSortedDynamicRow::PrimaryLockMask)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;
        auto dynamicRow = Store_->ModifyRow(row, lockMask, ERowModificationType::Write, &context);
        if (!dynamicRow) {
            return TSortedDynamicRow();
        }
        LockRow(transaction, prelock, dynamicRow);
        return dynamicRow;
    }

    TTimestamp WriteRow(
        const TUnversionedOwningRow& row,
        ui32 lockMask = TSortedDynamicRow::PrimaryLockMask)
    {
        auto transaction = StartTransaction();
        auto dynamicRow = WriteRow(transaction.get(), row, true, lockMask);
        if (!dynamicRow) {
            return NullTimestamp;
        }
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), dynamicRow);
        auto ts = CommitTransaction(transaction.get());
        CommitRow(transaction.get(), dynamicRow);
        return ts;
    }

    TSortedDynamicRow WriteRowNonAtomic(const TUnversionedOwningRow& row, TTimestamp timestamp)
    {
        TWriteContext context;
        context.Phase = EWritePhase::Commit;
        context.CommitTimestamp = timestamp;
        return Store_->ModifyRow(row, 0, ERowModificationType::Write, &context);
    }

    TSortedDynamicRow DeleteRow(
        TTransaction* transaction,
        const TOwningKey& key,
        bool prelock)
    {
        TWriteContext context;
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;
        auto dynamicRow = Store_->ModifyRow(key, TSortedDynamicRow::PrimaryLockMask, ERowModificationType::Delete, &context);
        LockRow(transaction, prelock, dynamicRow);
        return dynamicRow;
    }

    TTimestamp DeleteRow(const TOwningKey& key)
    {
        auto transaction = StartTransaction();
        auto row = DeleteRow(transaction.get(), key, true);
        if (!row) {
            return NullTimestamp;
        }
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), row);
        auto ts = CommitTransaction(transaction.get());
        CommitRow(transaction.get(), row);
        return ts;
    }

    TSortedDynamicRow DeleteRowNonAtomic(const TOwningKey& key, TTimestamp timestamp)
    {
        TWriteContext context;
        context.Phase = EWritePhase::Commit;
        context.CommitTimestamp = timestamp;
        return Store_->ModifyRow(key, TSortedDynamicRow::PrimaryLockMask, ERowModificationType::Delete, &context);
    }

    TUnversionedOwningRow LookupRow(const TOwningKey& key, TTimestamp timestamp)
    {
        return TSortedDynamicStoreTestBase::LookupRow(Store_, key, timestamp);
    }

    TSortedDynamicRow LookupDynamicRow(const TOwningKey& key)
    {
        return Store_->FindRow(key);
    }

    TTimestamp GetLastCommitTimestamp(TSortedDynamicRow row, int lockIndex = TSortedDynamicRow::PrimaryLockIndex)
    {
        return Store_->GetLastCommitTimestamp(row, lockIndex);
    }

    TTimestamp GetLastCommitTimestamp(const TOwningKey& key, int lockIndex = TSortedDynamicRow::PrimaryLockIndex)
    {
        auto row = LookupDynamicRow(key);
        EXPECT_TRUE(row);
        return GetLastCommitTimestamp(row, lockIndex);
    }


    TString DumpStore()
    {
        TStringBuilder builder;
        builder.AppendFormat("RowCount=%v ValueCount=%v MinTimestamp=%v MaxTimestamp=%v\n",
            Store_->GetRowCount(),
            Store_->GetValueCount(),
            Store_->GetMinTimestamp(),
            Store_->GetMaxTimestamp());

        int keyColumnCount = Tablet_->PhysicalSchema().GetKeyColumnCount();
        int schemaColumnCount = Tablet_->PhysicalSchema().GetColumnCount();
        int columnLockCount = Tablet_->GetColumnLockCount();
        for (auto row : Store_->GetAllRows()) {
            builder.AppendChar('[');
            for (int i = 0; i < keyColumnCount; ++i) {
                builder.AppendFormat(" %v", ToUnversionedValue(row.BeginKeys()[i], i));
            }
            builder.AppendString(" ] -> [");
            for (int i = keyColumnCount; i < schemaColumnCount; ++i) {
                for (auto list = row.GetFixedValueList(i, keyColumnCount, columnLockCount);
                     list;
                     list = list.GetSuccessor())
                {
                    EXPECT_FALSE(list.HasUncommitted());
                    for (int j = 0; j < list.GetSize(); ++j) {
                        const auto& dynamicValue = list[j];
                        TVersionedValue versionedValue;
                        static_cast<TUnversionedValue&>(versionedValue) = ToUnversionedValue(dynamicValue.Data, i);
                        versionedValue.Timestamp = Store_->TimestampFromRevision(dynamicValue.Revision);
                        builder.AppendFormat(" %v#%v", i, versionedValue);
                    }
                }
            }
            builder.AppendString("]");

            auto dumpTimestamps = [&] (TRevisionList list) {
                builder.AppendChar('[');
                while (list) {
                    EXPECT_FALSE(list.HasUncommitted());
                    for (int i = list.GetSize() - 1; i >= 0; --i) {
                        auto timestamp = Store_->TimestampFromRevision(list[i]);
                        builder.AppendFormat(" %v", timestamp);
                    }
                    list = list.GetSuccessor();
                }
                builder.AppendString(" ]");

            };

            for (int i = 0; i < columnLockCount; ++i) {
                auto& lock = row.BeginLocks(keyColumnCount)[i];
                builder.AppendFormat(" wts#%v: ", i);
                dumpTimestamps(TSortedDynamicRow::GetWriteRevisionList(lock));
            }

            builder.AppendString(" dts: ");
            dumpTimestamps(row.GetDeleteRevisionList(keyColumnCount, columnLockCount));

            builder.AppendChar('\n');
        }
        return builder.Flush();
    }


    TSortedDynamicStorePtr Store_;
    TClientBlockReadOptions BlockReadOptions_;

private:
    virtual void CreateDynamicStore() override
    {
        auto config = New<TTabletManagerConfig>();
        config->MaxBlockedRowWaitTime = TDuration::MilliSeconds(100);

        Store_ = New<TSortedDynamicStore>(
            config,
            TTabletId(),
            Tablet_.get());
    }

    virtual IDynamicStorePtr GetDynamicStore() override
    {
        return Store_;
    }


    TUnversionedValue ToUnversionedValue(const TDynamicValueData& data, int index)
    {
        TUnversionedValue value{};
        value.Id = index;
        value.Type = Tablet_->PhysicalSchema().Columns()[index].GetPhysicalType();
        if (IsStringLikeType(value.Type)) {
            value.Length = data.String->Length;
            value.Data.String = data.String->Data;
        } else {
            ::memcpy(&value.Data, &data, sizeof (data));
        }
        return value;
    }

    void LockRow(TTransaction* transaction, bool prelock, TSortedDynamicRow row)
    {
        auto rowRef = TSortedDynamicRowRef(Store_.Get(), nullptr, row);
        if (prelock) {
            transaction->PrelockedRows().push(rowRef);
        } else {
            transaction->LockedRows().push_back(rowRef);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSortedDynamicRowKeyComparerTest
    : public TSingleLockSortedDynamicStoreTest
    , public ::testing::WithParamInterface<std::tuple<const char*, const char*>>
{
public:
    virtual void SetUp() override
    {
        TSingleLockSortedDynamicStoreTest::SetUp();

        Transaction_ = StartTransaction();

        auto schema = GetSchema();
        int keyColumnCount = schema.GetKeyColumnCount();

        StaticComparer_ = TStaticComparer(schema);
        LlvmComparer_ = TSortedDynamicRowKeyComparer::Create(keyColumnCount, schema);
    }

    TSortedDynamicRow BuildDynamicRow(
        const TUnversionedOwningRow& row,
        ui32 lockMask = TSortedDynamicRow::PrimaryLockMask)
    {
        auto transaction = StartTransaction();
        auto dynamicRow = WriteRow(transaction.get(), row, false, lockMask);
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), dynamicRow);
        CommitTransaction(transaction.get());
        CommitRow(transaction.get(), dynamicRow);
        return dynamicRow;
    }

private:
    class TStaticComparer
    {
    public:
        TStaticComparer() = default;

        TStaticComparer(const TTableSchema& schema)
            : KeyColumnCount_(schema.GetKeyColumnCount())
            , Schema_(schema)
        { }

        int operator()(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const
        {
            return Compare(lhs, rhs);
        }

        int operator()(TSortedDynamicRow lhs, TUnversionedRowWrapper rhs) const
        {
            Y_ASSERT(rhs.Row.GetCount() >= KeyColumnCount_);
            return Compare(lhs, rhs.Row.Begin(), KeyColumnCount_);
        }

        int operator()(TSortedDynamicRow lhs, TKeyWrapper rhs) const
        {
            return Compare(lhs, rhs.Row.Begin(), rhs.Row.GetCount());
        }

        int operator()(
            const TUnversionedValue* lhsBegin,
            const TUnversionedValue* lhsEnd,
            const TUnversionedValue* rhsBegin,
            const TUnversionedValue* rhsEnd) const
        {
            return CompareRows(lhsBegin, lhsEnd, rhsBegin, rhsEnd);
        }

    private:
        int Compare(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const
        {
            ui32 nullKeyBit = 1;
            ui32 lhsNullKeyMask = lhs.GetNullKeyMask();
            ui32 rhsNullKeyMask = rhs.GetNullKeyMask();
            const auto* lhsValue = lhs.BeginKeys();
            const auto* rhsValue = rhs.BeginKeys();
            auto columnIt = Schema_.Columns().begin();
            for (int index = 0;
                 index < KeyColumnCount_;
                 ++index, nullKeyBit <<= 1, ++lhsValue, ++rhsValue, ++columnIt)
            {
                bool lhsNull = (lhsNullKeyMask & nullKeyBit) != 0;
                bool rhsNull = (rhsNullKeyMask & nullKeyBit) != 0;
                if (lhsNull && !rhsNull) {
                    return -1;
                } else if (!lhsNull && rhsNull) {
                    return +1;
                } else if (lhsNull && rhsNull) {
                    continue;
                }

                switch (columnIt->GetPhysicalType()) {
                    case EValueType::Int64: {
                        i64 lhsData = lhsValue->Int64;
                        i64 rhsData = rhsValue->Int64;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::Uint64: {
                        ui64 lhsData = lhsValue->Uint64;
                        ui64 rhsData = rhsValue->Uint64;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::Double: {
                        double lhsData = lhsValue->Double;
                        double rhsData = rhsValue->Double;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::Boolean: {
                        bool lhsData = lhsValue->Boolean;
                        bool rhsData = rhsValue->Boolean;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::String: {
                        size_t lhsLength = lhsValue->String->Length;
                        size_t rhsLength = rhsValue->String->Length;
                        size_t minLength = std::min(lhsLength, rhsLength);
                        int result = ::memcmp(lhsValue->String->Data, rhsValue->String->Data, minLength);
                        if (result != 0) {
                            return result;
                        } else if (lhsLength < rhsLength) {
                            return -1;
                        } else if (lhsLength > rhsLength) {
                            return +1;
                        }
                        break;
                    }

                    default:
                        Y_UNREACHABLE();
                }
            }
            return 0;
        }

        int Compare(TSortedDynamicRow lhs, const TUnversionedValue* rhsBegin, int rhsLength) const
        {
            ui32 nullKeyBit = 1;
            ui32 lhsNullKeyMask = lhs.GetNullKeyMask();
            const auto* lhsValue = lhs.BeginKeys();
            const auto* rhsValue = rhsBegin;

            auto columnIt = Schema_.Columns().begin();
            int lhsLength = KeyColumnCount_;
            int minLength = std::min(lhsLength, rhsLength);
            for (int index = 0;
                 index < minLength;
                 ++index, nullKeyBit <<= 1, ++lhsValue, ++rhsValue, ++columnIt)
            {
                auto lhsType = (lhsNullKeyMask & nullKeyBit) ? EValueType::Null : columnIt->GetPhysicalType();
                if (lhsType < rhsValue->Type) {
                    return -1;
                } else if (lhsType > rhsValue->Type) {
                    return +1;
                }

                switch (lhsType) {
                    case EValueType::Int64: {
                        i64 lhsData = lhsValue->Int64;
                        i64 rhsData = rhsValue->Data.Int64;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::Uint64: {
                        ui64 lhsData = lhsValue->Uint64;
                        ui64 rhsData = rhsValue->Data.Uint64;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::Double: {
                        double lhsData = lhsValue->Double;
                        double rhsData = rhsValue->Data.Double;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::Boolean: {
                        bool lhsData = lhsValue->Boolean;
                        bool rhsData = rhsValue->Data.Boolean;
                        if (lhsData < rhsData) {
                            return -1;
                        } else if (lhsData > rhsData) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::String: {
                        size_t lhsLength = lhsValue->String->Length;
                        size_t rhsLength = rhsValue->Length;
                        size_t minLength = std::min(lhsLength, rhsLength);
                        int result = ::memcmp(lhsValue->String->Data, rhsValue->Data.String, minLength);
                        if (result != 0) {
                            return result;
                        } else if (lhsLength < rhsLength) {
                            return -1;
                        } else if (lhsLength > rhsLength) {
                            return +1;
                        }
                        break;
                    }

                    case EValueType::Null:
                        break;

                    default:
                        Y_UNREACHABLE();
                }
            }
            return lhsLength - rhsLength;
        }

        int KeyColumnCount_;
        TTableSchema Schema_;
    };

protected:
    virtual TTableSchema GetSchema() const override
    {
        TTableSchema schema({
            TColumnSchema("a", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("b", EValueType::Uint64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("c", EValueType::Boolean)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("d", EValueType::Double)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("e", EValueType::String)
                .SetSortOrder(ESortOrder::Ascending)
        });
        return schema;
    }

    int Sign(int value) {
        return value != 0 ? value > 0 ? 1 : -1 : 0;
    }

    bool HasSentinels(TUnversionedOwningRow row) {
        for (int index = 0; index < row.GetCount(); ++index) {
            if (IsSentinelType(row[index].Type)) {
                return true;
            }
        }
        return false;
    }

    std::unique_ptr<TTransaction> Transaction_;
    TStaticComparer StaticComparer_;
    TSortedDynamicRowKeyComparer LlvmComparer_;
};

TEST_P(TSortedDynamicRowKeyComparerTest, Test)
{
    auto str1 = TString(std::get<0>(GetParam()));
    auto str2 = TString(std::get<1>(GetParam()));

    auto urow1 = BuildRow(str1, false);
    auto urow2 = BuildRow(str2, false);

    EXPECT_EQ(
        Sign(StaticComparer_(urow1.Begin(), urow1.End(), urow2.Begin(), urow2.End())),
        Sign(LlvmComparer_(urow1.Begin(), urow1.End(), urow2.Begin(), urow2.End())))
        << "row1: " << ToString(urow1) << std::endl
        << "row2: " << ToString(urow2);

    int keyColumnCount = GetSchema().GetKeyColumnCount();

    if (urow1.GetCount() == keyColumnCount && !HasSentinels(urow1) && !HasSentinels(urow2)) {
        auto drow1 = BuildDynamicRow(urow1);
        EXPECT_EQ(
            StaticComparer_(drow1, TKeyWrapper{urow2}),
            LlvmComparer_(drow1, TKeyWrapper{urow2}))
            << "row1: " << ToString(urow1) << std::endl
            << "row2: " << ToString(urow2);

        if (urow2.GetCount() == keyColumnCount) {
            auto drow2 = BuildDynamicRow(urow2);
            EXPECT_EQ(StaticComparer_(drow1, drow2), LlvmComparer_(drow1, drow2))
                << "row1: " << ToString(urow1) << std::endl
                << "row2: " << ToString(urow2);
        }
    }
}

auto comparerTestParams = ::testing::Values(
    "a=10;b=18446744073709551615u;c=%false;d=3.14;e=\"str1\"",
    "a=10;b=18446744073709551615u;c=%false;d=3.14;e=\"str2\"",
    "a=10;b=18446744073709551615u;c=%false;d=2.71;e=\"str2\"",
    "a=10;b=18446744073709551615u;c=%true;d=3.14;e=\"str2\"",
    "a=10;b=18446744073709551614u;c=%false;d=3.14;e=\"str2\"",
    "a=11;b=18446744073709551615u;c=%false;d=3.14;e=\"str2\"",
    "a=10;b=18446744073709551615u;c=%false;d=3.14",
    "a=10;b=18446744073709551615u;c=%false;d=3.15",
    "a=10;b=18446744073709551615u;c=%true",
    "a=10;b=18446744073709551614u",
    "a=12",
    "a=10",
    "a=<\"type\"=\"min\">#",
    "a=<\"type\"=\"max\">#");

INSTANTIATE_TEST_CASE_P(
    CodeGenerationTest,
    TSortedDynamicRowKeyComparerTest,
    ::testing::Combine(comparerTestParams, comparerTestParams));

TEST_F(TSortedDynamicRowKeyComparerTest, DifferentLength)
{
    auto row1 = BuildKey("1");
    auto row2 = BuildKey("1;<\"type\"=\"min\">#");

    EXPECT_EQ(
        Sign(StaticComparer_(row1.Begin(), row1.End(), row2.Begin(), row2.End())),
        Sign(LlvmComparer_(row1.Begin(), row1.End(), row2.Begin(), row2.End())))
        << "row1: " << ToString(row1) << std::endl
        << "row2: " << ToString(row2);
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSingleLockSortedDynamicStoreTest, Empty)
{
    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 0), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));
}

TEST_F(TSingleLockSortedDynamicStoreTest, PrelockWriteAndCommit)
{
    auto transaction = StartTransaction();

    auto key = BuildKey("1");

    auto rowString = "key=1;a=1";

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));

    auto row = WriteRow(transaction.get(), BuildRow(rowString), true);
    ASSERT_FALSE(row.GetDeleteLockFlag());
    const auto& lock = GetLock(row);
    ASSERT_EQ(transaction.get(), lock.Transaction);
    ASSERT_TRUE(transaction->LockedRows().empty());

    ConfirmRow(transaction.get(), row);
    ASSERT_EQ(1, transaction->LockedRows().size());
    ASSERT_TRUE(transaction->LockedRows()[0].Row == row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    auto ts = CommitTransaction(transaction.get());
    CommitRow(transaction.get(), row);

    ASSERT_FALSE(row.GetDeleteLockFlag());
    ASSERT_EQ(nullptr, lock.Transaction);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts - 1), nullptr));
}

TEST_F(TSingleLockSortedDynamicStoreTest, PrelockDeleteAndCommit)
{
    auto key = BuildKey("1");
    auto rowString = "key=1;a=1";

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

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), nullptr));
}

TEST_F(TSingleLockSortedDynamicStoreTest, PrelockManyWritesAndCommit)
{
    auto key = BuildKey("1");

    std::vector<TTimestamp> timestamps;

    for (int i = 0; i < 100; ++i) {
        auto transaction = StartTransaction();

        if (i == 0) {
            EXPECT_TRUE(AreRowsEqual(LookupRow(key, transaction->GetStartTimestamp()), nullptr));
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


    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), "key=1;a=99"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;a=99"));

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, timestamps[i]), TString("key=1;a=" + ToString(i))));
    }
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteSameRow)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    WriteRow(transaction.get(), BuildRow("key=1;b=3.14"), true);
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction.get(), BuildRow("key=1;b=2.71"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteAndAbort)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;b=3.14"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    AbortTransaction(transaction.get());
    AbortRow(transaction.get(), row);

    EXPECT_EQ(nullptr, GetLock(row).Transaction);
}

TEST_F(TSingleLockSortedDynamicStoreTest, Delete)
{
    auto key = BuildKey("1");
    auto ts = DeleteRow(key);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteDelete)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;c=value"));

    auto transaction2 = StartTransaction();

    auto row = DeleteRow(transaction2.get(), key, false);
    EXPECT_EQ(ts1, GetLastCommitTimestamp(row));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;c=value"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;c=value"));

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;c=value"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), nullptr));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(row));
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteWrite)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;a=1"));

    auto transaction2 = StartTransaction();

    auto row = WriteRow(transaction2.get(), BuildRow("key=1;b=3.14"), false);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
}

TEST_F(TSingleLockSortedDynamicStoreTest, DeleteSameRow)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    DeleteRow(transaction.get(), key, true);
    EXPECT_EQ(TSortedDynamicRow(), DeleteRow(transaction.get(), key, true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, Update1)
{
    auto key = BuildKey("1");

    auto ts = WriteRow(BuildRow("key=1;a=1", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), "key=1;a=1"));
}

TEST_F(TSingleLockSortedDynamicStoreTest, Update2)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts2 = WriteRow(BuildRow("key=1;b=3.0", false));
    auto ts3 = WriteRow(BuildRow("key=1;c=test", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1;b=3.0"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), "key=1;a=1;b=3.0;c=test"));
}

TEST_F(TSingleLockSortedDynamicStoreTest, Update3)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts2 = WriteRow(BuildRow("key=1;a=2", false));
    auto ts3 = WriteRow(BuildRow("key=1;a=3", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=2"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), "key=1;a=3"));
}

TEST_F(TSingleLockSortedDynamicStoreTest, UpdateDelete1)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts2 = DeleteRow(key);
    auto ts3 = WriteRow(BuildRow("key=1;b=2.0", false));
    auto ts4 = DeleteRow(key);
    auto ts5 = WriteRow(BuildRow("key=1;c=test", false));
    auto ts6 = DeleteRow(key);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), "key=1;b=2.0"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts4), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts5), "key=1;c=test"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts6), nullptr));
}

TEST_F(TSingleLockSortedDynamicStoreTest, UpdateDelete2)
{
    auto key = BuildKey("1");

    auto ts1 = DeleteRow(key);
    auto ts2 = DeleteRow(key);
    auto ts3 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts4 = DeleteRow(key);
    auto ts5 = DeleteRow(key);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts4), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts5), nullptr));
}

TEST_F(TSingleLockSortedDynamicStoreTest, DeleteAfterWriteFailure1)
{
    auto transaction = StartTransaction();
    WriteRow(transaction.get(), BuildRow("key=1"), true);
    EXPECT_EQ(TSortedDynamicRow(), DeleteRow(transaction.get(), BuildKey("1"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, DeleteAfterWriteFailure2)
{
    WriteRow(BuildRow("key=1"));
    auto transaction = StartTransaction();
    WriteRow(transaction.get(), BuildRow("key=1"), true);
    EXPECT_EQ(TSortedDynamicRow(), DeleteRow(transaction.get(), BuildKey("1"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteAfterDeleteFailure1)
{
    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), BuildKey("1"), true);
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction.get(), BuildRow("key=1"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteAfterDeleteFailure2)
{
    WriteRow(BuildRow("key=1"));
    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), BuildKey("1"), true);
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction.get(), BuildRow("key=1"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;c=test1"), true);
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;c=test2"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteWriteConflict2)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row = WriteRow(transaction1.get(), BuildRow("key=1;a=1"), true);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row);

    CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), row);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;a=2"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadNotBlocked)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (TSortedDynamicRow /*row*/, int /*lockIndex*/) {
        blocked = true;
    }));

    // Not blocked.
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, transaction->GetPrepareTimestamp()), nullptr));

    EXPECT_FALSE(blocked);
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadBlockedAbort)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (TSortedDynamicRow blockedRow, int lockIndex) {
        EXPECT_EQ(TSortedDynamicRow::PrimaryLockIndex, lockIndex);
        EXPECT_EQ(blockedRow, row);
        AbortTransaction(transaction.get());
        AbortRow(transaction.get(), row);
        blocked = true;
    }));

    // Blocked, old value is read.
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(blocked);
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadBlockedCommit)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (TSortedDynamicRow blockedRow, int lockIndex) {
        EXPECT_EQ(TSortedDynamicRow::PrimaryLockIndex, lockIndex);
        EXPECT_EQ(blockedRow, row);
        CommitTransaction(transaction.get());
        CommitRow(transaction.get(), row);
        blocked = true;
    }));

    // Blocked, new value is read.
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(blocked);
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadBlockedTimeout)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (TSortedDynamicRow blockedRow, int lockIndex) {
        blocked = true;
        Sleep(TDuration::MilliSeconds(10));
    }));

    // Blocked, timeout.
    EXPECT_ANY_THROW({
        LookupRow(key, SyncLastCommittedTimestamp);
    });
    EXPECT_TRUE(blocked);
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteNotBlocked)
{
    auto row = BuildRow("key=1;a=1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto dynamicRow = WriteRow(transaction1.get(), row, false);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow);

    // Not blocked, write conflicted.
    TWriteContext context;
    context.Phase = EWritePhase::Prelock;
    context.Transaction = transaction2.get();
    EXPECT_EQ(TSortedDynamicRow(), Store_->ModifyRow(row, TSortedDynamicRow::PrimaryLockMask, ERowModificationType::Write, &context));
    EXPECT_FALSE(context.Error.IsOK());
}

TEST_F(TSingleLockSortedDynamicStoreTest, WriteBlocked)
{
    auto row = BuildRow("key=1;a=1");

    auto transaction1 = StartTransaction();

    auto dynamicRow = WriteRow(transaction1.get(), row, false);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow);

    auto transaction2 = StartTransaction();

    // Blocked, no value is written.
    TWriteContext context;
    context.Phase = EWritePhase::Prelock;
    context.Transaction = transaction2.get();
    EXPECT_EQ(TSortedDynamicRow(), Store_->ModifyRow(row, TSortedDynamicRow::PrimaryLockMask, ERowModificationType::Write, &context));
    EXPECT_EQ(dynamicRow, context.BlockedRow);
}

TEST_F(TSingleLockSortedDynamicStoreTest, ArbitraryKeyLength)
{
    WriteRow(BuildRow("key=1;a=1"));

    auto reader = Store_->CreateReader(
        Tablet_->BuildSnapshot(nullptr),
        MakeSingletonRowRange(BuildKey("1"), BuildKey("1;<type=max>#")),
        AsyncLastCommittedTimestamp,
        false,
        TColumnFilter(),
        BlockReadOptions_);

    EXPECT_TRUE(reader->Open().Get().IsOK());

    std::vector<TVersionedRow> rows;
    rows.reserve(10);

    EXPECT_TRUE(reader->Read(&rows));
    EXPECT_EQ(1, rows.size());

    EXPECT_FALSE(reader->Read(&rows));
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeEmpty)
{
    auto check = [&] () {
        EXPECT_EQ(0, Store_->GetRowCount());
        EXPECT_EQ(0, Store_->GetValueCount());
        EXPECT_EQ(MaxTimestamp, Store_->GetMinTimestamp());
        EXPECT_EQ(MinTimestamp, Store_->GetMaxTimestamp());
    };

    check();

    ReserializeStore();

    check();
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeNonempty1)
{
    std::vector<TTimestamp> timestamps;
    for (int i = 0; i < 100; ++i) {
        auto timestamp = WriteRow(BuildRow(Format("key=%v;a=%v", i, i + 100), false));
        timestamps.push_back(timestamp);
    }

    auto check = [&] () {
        EXPECT_EQ(100, Store_->GetRowCount());
        EXPECT_EQ(100, Store_->GetValueCount());
        EXPECT_EQ(timestamps[0], Store_->GetMinTimestamp());
        EXPECT_EQ(timestamps[99], Store_->GetMaxTimestamp());

        for (int i = 0; i < 100; ++i) {
            auto key = BuildKey(Format("%v", i));

            EXPECT_TRUE(AreRowsEqual(
                LookupRow(key, MaxTimestamp),
                Format("key=%v;a=%v", i, i + 100)));

            EXPECT_EQ(timestamps[i], GetLastCommitTimestamp(key));
        }
    };

    check();

    ReserializeStore();

    check();
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeNonempty2)
{
    auto key = BuildKey("1");
    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));
    auto ts2 = WriteRow(BuildRow("key=1;c=test", false));
    auto ts3 = DeleteRow(key);

    auto check = [&] () {
        EXPECT_EQ(1, Store_->GetRowCount());
        EXPECT_EQ(2, Store_->GetValueCount());
        EXPECT_EQ(ts1, Store_->GetMinTimestamp());
        EXPECT_EQ(ts3, Store_->GetMaxTimestamp());

        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1 - 1), nullptr));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1;c=test"));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), nullptr));

        EXPECT_EQ(ts3, GetLastCommitTimestamp(key));
    };

    check();

    ReserializeStore();

    check();
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeSnapshot1)
{
    auto snapshot = BeginReserializeStore();

    WriteRow(BuildRow("key=1;a=1", false));

    EXPECT_EQ(1, Store_->GetRowCount());

    EndReserializeStore(snapshot);

    EXPECT_EQ(0, Store_->GetRowCount());
    EXPECT_EQ(0, Store_->GetValueCount());
    EXPECT_EQ(MaxTimestamp, Store_->GetMinTimestamp());
    EXPECT_EQ(MinTimestamp, Store_->GetMaxTimestamp());
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeSnapshot2)
{
    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));

    EXPECT_EQ(1, Store_->GetRowCount());

    auto snapshot = BeginReserializeStore();
    auto dump = DumpStore();

    WriteRow(BuildRow("key=2;a=2", false));

    EXPECT_EQ(2, Store_->GetRowCount());
    EXPECT_EQ(2, Store_->GetValueCount());

    EndReserializeStore(snapshot);
    EXPECT_EQ(dump, DumpStore());

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(1, Store_->GetValueCount());
    EXPECT_EQ(ts1, Store_->GetMinTimestamp());
    EXPECT_EQ(ts1, Store_->GetMaxTimestamp());

    auto key1 = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key1, ts1 - 1), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key1, ts1), "key=1;a=1"));

    auto key2 = BuildKey("2");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key2, MaxTimestamp), nullptr));

    EXPECT_EQ(ts1, GetLastCommitTimestamp(key1));
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeSnapshot3)
{
    auto ts1 = WriteRow(BuildRow("key=1;a=1", false));

    EXPECT_EQ(1, Store_->GetRowCount());

    auto snapshot = BeginReserializeStore();
    auto dump = DumpStore();

    auto ts2 = WriteRow(BuildRow("key=1;a=2", false));

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(2, Store_->GetValueCount());

    EndReserializeStore(snapshot);
    EXPECT_EQ(dump, DumpStore());

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(1, Store_->GetValueCount());
    EXPECT_EQ(ts1, Store_->GetMinTimestamp());
    EXPECT_EQ(ts1, Store_->GetMaxTimestamp());

    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1 - 1), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), "key=1;a=1"));

    EXPECT_EQ(ts1, GetLastCommitTimestamp(key));
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeSnapshot4)
{
    auto key = BuildKey("1");

    auto ts1 = WriteRow(BuildRow("key=1;a=1;b=3.14", false));

    EXPECT_EQ(1, Store_->GetRowCount());

    auto snapshot = BeginReserializeStore();
    auto dump = DumpStore();

    auto ts2 = DeleteRow(key);

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(2, Store_->GetValueCount());

    EndReserializeStore(snapshot);
    EXPECT_EQ(dump, DumpStore());

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(2, Store_->GetValueCount());
    EXPECT_EQ(ts1, Store_->GetMinTimestamp());
    EXPECT_EQ(ts1, Store_->GetMaxTimestamp());

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1 - 1), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), "key=1;a=1;b=3.14"));

    EXPECT_EQ(ts1, GetLastCommitTimestamp(key));
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeSnapshot5)
{
    auto key = BuildKey("1");

    std::vector<TTimestamp> timestamps;
    TStoreSnapshot snapshot;
    for (int i = 0; i < 100; ++i) {
        auto timestamp = WriteRow(BuildRow(Format("key=1;a=%v", i + 100), false));
        timestamps.push_back(timestamp);
        if (i == 50) {
            snapshot = BeginReserializeStore();
        }
    }

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(100, Store_->GetValueCount());
    EXPECT_EQ(timestamps[0], Store_->GetMinTimestamp());
    EXPECT_EQ(timestamps[99], Store_->GetMaxTimestamp());

    EndReserializeStore(snapshot);

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(51, Store_->GetValueCount());
    EXPECT_EQ(timestamps[0], Store_->GetMinTimestamp());
    EXPECT_EQ(timestamps[50], Store_->GetMaxTimestamp());

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(AreRowsEqual(
            LookupRow(key, timestamps[i]),
            Format("key=1;a=%v", std::min(i + 100, 150))));
    }

    EXPECT_EQ(timestamps[50], GetLastCommitTimestamp(key));
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeSnapshot_YT2591)
{
    auto transaction1 = StartTransaction();
    auto row1 = WriteRow(transaction1.get(), BuildRow("key=1;b=2.7", false), false);
    auto row2 = WriteRow(transaction1.get(), BuildRow("key=2;b=3.1", false), false);
    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row1);
    PrepareRow(transaction1.get(), row2);
    CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), row1);
    CommitRow(transaction1.get(), row2);

    auto transaction2 = StartTransaction();
    auto row1_ = WriteRow(transaction2.get(), BuildRow("key=1;a=1", false), false);
    EXPECT_EQ(row1, row1_);
    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row1);
    CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row1);

    auto dump = DumpStore();
    ReserializeStore();
    EXPECT_EQ(dump, DumpStore());
}

////////////////////////////////////////////////////////////////////////////////

class TMultiLockSortedDynamicStoreTest
    : public TSingleLockSortedDynamicStoreTest
{
protected:
    virtual TTableSchema GetSchema() const
    {
        // NB: Key columns must go first.
        TTableSchema schema({
            TColumnSchema(TColumnSchema("key", EValueType::Int64).SetSortOrder(ESortOrder::Ascending)),
            TColumnSchema(TColumnSchema("a", EValueType::Int64).SetLock(TString("l1"))),
            TColumnSchema(TColumnSchema("b", EValueType::Double).SetLock(TString("l2"))),
            TColumnSchema(TColumnSchema("c", EValueType::String))
        });
        return schema;
    }

    static const ui32 LockMask1 = 1 << 1;
    static const ui32 LockMask2 = 1 << 2;

};

TEST_F(TMultiLockSortedDynamicStoreTest, ConcurrentWrites1)
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

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(row, 1));

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(row, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(row, 2));
}

TEST_F(TMultiLockSortedDynamicStoreTest, ConcurrentWrites2)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();

    auto transaction2 = StartTransaction();
    auto row = WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), true, LockMask2);

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(row, 2));

    EXPECT_EQ(row, WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), true, LockMask1));

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row);

    auto ts1 = CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(row, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(row, 2));
}

TEST_F(TMultiLockSortedDynamicStoreTest, ConcurrentWrites3)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row1 = WriteRow(transaction1.get(), BuildRow("key=1;b=3.14", false), true, LockMask2);
    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row1);

    auto row2 = WriteRow(transaction2.get(), BuildRow("key=1;a=1", false), true, LockMask1);
    EXPECT_EQ(row1, row2);
    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row2);

    AbortTransaction(transaction1.get());
    AbortRow(transaction1.get(), row1);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row2);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row2));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(row2, 1));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row2, 2));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), true, LockMask1);

    auto transaction2 = StartTransaction();
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;a=2", false), true, LockMask1));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteWriteConflict2)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;a=1;b=3.14", false), true, LockMask1|LockMask2);

    auto transaction2 = StartTransaction();
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;a=2", false), true, LockMask1));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteWriteConflict3)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;c=test", false), true, TSortedDynamicRow::PrimaryLockMask);

    auto transaction2 = StartTransaction();
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;a=1", false), true, LockMask1));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteWriteConflict4)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    WriteRow(BuildRow("key=1;a=1;b=3.14", false));

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction.get(), BuildRow("key=1;a=1", false), true, LockMask1));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteDeleteConflict1)
{
    auto key = BuildKey("1");

auto transaction = StartTransaction();
    WriteRow(transaction.get(), BuildRow("key=1;a=1", false), true, LockMask1);

    EXPECT_EQ(NullTimestamp, DeleteRow(key));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteDeleteConflict2)
{
    auto key = BuildKey("1");

auto transaction = StartTransaction();
    WriteRow(BuildRow("key=1;a=1", false), LockMask1);

    EXPECT_EQ(TSortedDynamicRow(), DeleteRow(transaction.get(), key, true));
}

TEST_F(TMultiLockSortedDynamicStoreTest, DeleteWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    DeleteRow(key);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction.get(), BuildRow("key=1;a=1", false), true, LockMask1));
}

TEST_F(TMultiLockSortedDynamicStoreTest, DeleteWriteConflict2)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), key, true);

    EXPECT_EQ(NullTimestamp, WriteRow(BuildRow("key=1;a=1", false), LockMask1));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteNotBlocked)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row1 = WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), false, LockMask1);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row1);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (TSortedDynamicRow /*blockedRow*/, int /*lockIndex*/) {
        blocked = true;
    }));

    // Not blocked, not conflicted.
    auto row2 = WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), true, LockMask2);
    EXPECT_EQ(row1, row2);
    EXPECT_FALSE(blocked);
}

TEST_F(TMultiLockSortedDynamicStoreTest, OutOfOrderWrites)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row1 = WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), false, LockMask1);
    auto row2 = WriteRow(transaction2.get(), BuildRow("key=1;b=3.14", false), false, LockMask2);
    EXPECT_EQ(row1, row2);
    auto row = row1;

    // Mind the order!
    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row1);

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), row2);

    auto ts2 = CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), row2);

    auto ts1 = CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), row1);

    EXPECT_LE(ts2, ts1);

    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(row));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(row, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(row, 2));

    {
        auto reader = Store_->CreateSnapshotReader();
        reader->Open()
            .Get()
            .ThrowOnError();

        std::vector<TVersionedRow> rows;
        rows.reserve(1);
        EXPECT_TRUE(reader->Read(&rows));
        EXPECT_EQ(1, rows.size());

        auto row = rows[0];
        EXPECT_EQ(1, row.GetKeyCount());
        EXPECT_EQ(2, row.GetValueCount());
        EXPECT_EQ(2, row.GetWriteTimestampCount());
        EXPECT_EQ(ts1, row.BeginWriteTimestamps()[0]);
        EXPECT_EQ(ts2, row.BeginWriteTimestamps()[1]);
        EXPECT_EQ(0, row.GetDeleteTimestampCount());

        EXPECT_FALSE(reader->Read(&rows));
        EXPECT_TRUE(rows.empty());
    }
}

TEST_F(TMultiLockSortedDynamicStoreTest, SerializeSnapshot1)
{
    auto key = BuildKey("1");

    auto ts1 = DeleteRow(key);
    auto ts2 = WriteRow(BuildRow("key=1;a=1", false), LockMask1);
    auto ts3 = WriteRow(BuildRow("key=1;c=test", false), TSortedDynamicRow::PrimaryLockMask);
    auto ts4 = WriteRow(BuildRow("key=1;b=3.14", false), LockMask2);

    auto check = [&] () {
        EXPECT_EQ(1, Store_->GetRowCount());
        EXPECT_EQ(3, Store_->GetValueCount());

        auto row = LookupDynamicRow(key);
        EXPECT_EQ(ts3, GetLastCommitTimestamp(row));
        EXPECT_EQ(ts3, GetLastCommitTimestamp(row, 1));
        EXPECT_EQ(ts4, GetLastCommitTimestamp(row, 2));

        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1 - 1), nullptr));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), nullptr));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1"));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts3), "key=1;a=1;c=test"));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts4), "key=1;a=1;b=3.14;c=test"));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), "key=1;a=1;b=3.14;c=test"));
    };

    check();

    auto dump = DumpStore();
    ReserializeStore();
    EXPECT_EQ(dump, DumpStore());

    check();
}

////////////////////////////////////////////////////////////////////////////////

class TNonAtomicSortedDynamicStoreTest
    : public TSingleLockSortedDynamicStoreTest
{
protected:
    virtual EAtomicity GetAtomicity() const override
    {
        return EAtomicity::None;
    }
};

TEST_F(TNonAtomicSortedDynamicStoreTest, Write1)
{
    auto key = BuildKey("1");

    auto row = WriteRowNonAtomic(BuildRow("key=1;a=1", false), 100);

    EXPECT_EQ(100, GetLastCommitTimestamp(row));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;a=1"));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, Write2)
{
    auto key = BuildKey("1");
    auto rowStr1 = "key=1;a=1";
    auto rowStr2 = "key=1;b=3.14";

    auto row1 = WriteRowNonAtomic(BuildRow(rowStr1, false), 100);
    EXPECT_EQ(100, GetLastCommitTimestamp(row1));

    auto row2 = WriteRowNonAtomic(BuildRow(rowStr2, false), 200);
    EXPECT_EQ(200, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 200), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 300), "key=1;a=1;b=3.14"));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, Write3)
{
    auto key = BuildKey("1");

    std::vector<TTimestamp> timestamps;
    for (int i = 0; i < 100; ++i) {
        auto ts = GenerateTimestamp();
        timestamps.push_back(ts);

        if (i == 0) {
            EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), nullptr));
        } else {
            EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), "key=1;a=" + ToString(i - 1)));
        }

        WriteRowNonAtomic(BuildRow("key=1;a=" + ToString(i)), ts);
    }

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), "key=1;a=99"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;a=99"));

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, timestamps[i]), TString("key=1;a=" + ToString(i))));
    }
}

TEST_F(TNonAtomicSortedDynamicStoreTest, Delete1)
{
    auto key = BuildKey("1");
    auto row = DeleteRowNonAtomic(key, 100);

    EXPECT_EQ(100, GetLastCommitTimestamp(row));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, Delete2)
{
    auto key = BuildKey("1");

    auto row1 = DeleteRowNonAtomic(key, 100);
    EXPECT_EQ(100, GetLastCommitTimestamp(row1));

    auto row2 = DeleteRowNonAtomic(key, 200);
    EXPECT_EQ(200, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, WriteDelete1)
{
    auto key = BuildKey("1");

    auto row1 = WriteRowNonAtomic(BuildRow("key=1;a=1", false), 100);
    EXPECT_EQ(100, GetLastCommitTimestamp(row1));

    auto row2 = DeleteRowNonAtomic(key, 200);
    EXPECT_EQ(200, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 200), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, WriteDelete2)
{
    auto key = BuildKey("1");

    auto row1 = DeleteRowNonAtomic(key, 100);
    EXPECT_EQ(100, GetLastCommitTimestamp(row1));

    auto row2 = WriteRowNonAtomic(BuildRow("key=1;a=1", false), 200);
    EXPECT_EQ(200, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 200), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), "key=1;a=1"));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, WriteDelete3)
{
    std::vector<TTimestamp> writeTimestamps;
    for (int i = 0; i < 100; ++i) {
        auto ts = GenerateTimestamp();
        writeTimestamps.push_back(ts);
        WriteRowNonAtomic(BuildRow(Format("key=%v;a=%v", i, i)), ts);
    }

    std::vector<TTimestamp> deleteTimestamps;
    for (int i = 0; i < 100; ++i) {
        auto ts = GenerateTimestamp();
        deleteTimestamps.push_back(ts);
        DeleteRowNonAtomic(BuildKey(ToString(i)), ts);
    }

    for (int i = 0; i < 100; ++i) {
        auto key = BuildKey(ToString(i));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, writeTimestamps[i] - 1), nullptr));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, writeTimestamps[i]), Format("key=%v;a=%v", i, i)));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, deleteTimestamps[i] - 1), Format("key=%v;a=%v", i, i)));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, deleteTimestamps[i]), nullptr));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

