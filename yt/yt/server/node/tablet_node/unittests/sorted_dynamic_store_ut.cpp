#include "sorted_dynamic_store_ut_helpers.h"

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>

#include <yt/yt/library/row_merger/row_merger.h>

#include <yt/yt/client/transaction_client/ts_literal.h>

#include <util/system/thread.h>

#include <array>
#include <bitset>

namespace NYT::NTabletNode {
namespace {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;

using NTransactionClient::operator""_ts;

TTimestamp PrecedingTimestamp(TTimestamp timestamp)
{
    return TTimestamp(timestamp.Underlying() - 1);
}

////////////////////////////////////////////////////////////////////////////////

class TSingleLockSortedDynamicStoreTest
    : public TSortedStoreTestBase
{
protected:
    TSortedDynamicStorePtr Store_;

    void SetUp() override
    {
        TSortedStoreTestBase::SetUp();
        CreateDynamicStore();
    }

    void ConfirmRow(TTestTransaction* transaction, TSortedDynamicRow row)
    {
        transaction->LockedRows().push_back(TSortedDynamicRowRef(Store_.Get(), nullptr, row));
    }

    void PrepareRow(TTransaction* transaction, TSortedDynamicRow row)
    {
        Store_->PrepareRow(transaction, row);
    }

    struct TSortedDynamicRowWithLock
        : public TSortedDynamicRow
    {
        TSortedDynamicRowWithLock(TSortedDynamicRow base)
            : TSortedDynamicRow(base)
        { }

        TSortedDynamicRowWithLock(TSortedDynamicRow base, TLockMask lockMask)
            : TSortedDynamicRow(base)
            , LockMask(lockMask)
        { }

        TLockMask LockMask;
    };

    void WriteRow(TTransaction* transaction, TSortedDynamicRowWithLock dynamicRow, TUnversionedRow row)
    {
        Store_->WriteRow(transaction, dynamicRow, row);
    }

    void DeleteRow(TTransaction* transaction, TSortedDynamicRowWithLock dynamicRow)
    {
        Store_->DeleteRow(transaction, dynamicRow);
    }

    void CommitRow(TTransaction* transaction, TSortedDynamicRowWithLock row, bool commandIsPureLock = false)
    {
        Store_->CommitRow(transaction, row, row.LockMask, commandIsPureLock);
    }

    void AbortRow(TTransaction* transaction, TSortedDynamicRowWithLock row)
    {
        Store_->AbortRow(transaction, row, row.LockMask);
    }

    TSortedDynamicRowWithLock ModifyRow(
        TTestTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock,
        TLockMask lockMask)
    {
        // Enrich with write locks
        {
            const auto& columnIndexToLockIndex = Tablet_->ColumnIndexToLockIndex();
            int keyColumnCount = Tablet_->GetPhysicalSchema()->GetKeyColumnCount();

            for (int index = keyColumnCount; index < row.GetCount(); ++index) {
                const auto& value = row[index];
                int lockIndex = columnIndexToLockIndex[value.Id];
                lockMask.Set(lockIndex, ELockType::Exclusive);
            }
        }

        auto context = transaction->CreateWriteContext();
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        context.Transaction = transaction;
        auto dynamicRow = Store_->ModifyRow(row, lockMask, false, &context);
        if (!dynamicRow) {
            return TSortedDynamicRow();
        }
        LockRow(transaction, prelock, dynamicRow);
        return TSortedDynamicRowWithLock(dynamicRow, lockMask);
    }

    TSortedDynamicRowWithLock ModifyRow(
        TTestTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock)
    {
        TLockMask lockMask;
        lockMask.Set(PrimaryLockIndex, ELockType::SharedWeak);
        return ModifyRow(transaction, row, prelock, lockMask);
    }

    TSortedDynamicRowWithLock LockRow(
        TTestTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock,
        TLockMask lockMask)
    {
        return ModifyRow(transaction, row, prelock, lockMask);
    }

    TSortedDynamicRowWithLock LockRow(
        TTestTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock,
        ELockType lockType = ELockType::SharedWeak)
    {
        TLockMask lockMask;
        lockMask.Set(PrimaryLockIndex, lockType);
        return LockRow(transaction, row, prelock, lockMask);
    }

    TSortedDynamicRowWithLock WriteRow(
        TTestTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock,
        TLockMask lockMask)
    {
        return ModifyRow(transaction, row, prelock, lockMask);
    }

    TSortedDynamicRowWithLock WriteRow(
        TTestTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock)
    {
        TLockMask lockMask;
        lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
        return WriteRow(transaction, row, prelock, lockMask);
    }

    TTimestamp WriteRow(
        const TUnversionedOwningRow& row,
        TLockMask lockMask)
    {
        auto transaction = StartTransaction();
        auto dynamicRow = WriteRow(transaction.get(), row, true, lockMask);
        if (!dynamicRow) {
            return NullTimestamp;
        }
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), dynamicRow);
        auto ts = CommitTransaction(transaction.get());
        WriteRow(transaction.get(), dynamicRow, row);
        CommitRow(transaction.get(), dynamicRow);
        return ts;
    }

    TTimestamp WriteRow(
        const TUnversionedOwningRow& row)
    {
        TLockMask lockMask;
        lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
        return WriteRow(row, lockMask);
    }

    TSortedDynamicRowWithLock WriteRowNonAtomic(const TUnversionedOwningRow& row, TTimestamp timestamp)
    {
        TWriteContext context;
        context.Phase = EWritePhase::Commit;
        context.CommitTimestamp = timestamp;
        return Store_->ModifyRow(row, TLockMask(), false, &context);
    }

    TSortedDynamicRowWithLock DeleteRow(
        TTestTransaction* transaction,
        const TLegacyOwningKey& key,
        bool prelock)
    {
        auto context = transaction->CreateWriteContext();
        context.Phase = prelock ? EWritePhase::Prelock : EWritePhase::Lock;
        TLockMask lockMask;
        lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
        auto dynamicRow = Store_->ModifyRow(key, lockMask, true, &context);
        LockRow(transaction, prelock, dynamicRow);
        return TSortedDynamicRowWithLock(dynamicRow, lockMask);
    }

    TTimestamp DeleteRow(const TLegacyOwningKey& key)
    {
        auto transaction = StartTransaction();
        auto row = DeleteRow(transaction.get(), key, true);
        if (!row) {
            return NullTimestamp;
        }
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), row);
        auto ts = CommitTransaction(transaction.get());
        DeleteRow(transaction.get(), row);
        CommitRow(transaction.get(), row);
        return ts;
    }

    TSortedDynamicRowWithLock DeleteRowNonAtomic(const TLegacyOwningKey& key, TTimestamp timestamp)
    {
        TWriteContext context;
        context.Phase = EWritePhase::Commit;
        context.CommitTimestamp = timestamp;
        TLockMask lockMask;
        lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
        return Store_->ModifyRow(key, lockMask, true, &context);
    }

    TUnversionedOwningRow LookupRow(const TLegacyOwningKey& key, TTimestamp timestamp)
    {
        return TSortedStoreTestBase::LookupRow(Store_, key, timestamp);
    }

    TSortedDynamicRowWithLock LookupDynamicRow(const TLegacyOwningKey& key)
    {
        return Store_->FindRow(key);
    }

    TTimestamp GetLastCommitTimestamp(TSortedDynamicRow row, int lockIndex = PrimaryLockIndex)
    {
        return std::max(
            Store_->GetLastWriteTimestamp(row, 0),
            Store_->GetLastWriteTimestamp(row, lockIndex));
    }

    TTimestamp GetLastCommitTimestamp(const TLegacyOwningKey& key, int lockIndex = PrimaryLockIndex)
    {
        auto row = LookupDynamicRow(key);
        EXPECT_TRUE(row);
        return GetLastCommitTimestamp(row, lockIndex);
    }

    std::string DumpStore()
    {
        TStringBuilder builder;
        builder.AppendFormat("RowCount=%v ValueCount=%v MinTimestamp=%v MaxTimestamp=%v\n",
            Store_->GetRowCount(),
            Store_->GetValueCount(),
            Store_->GetMinTimestamp(),
            Store_->GetMaxTimestamp());

        int keyColumnCount = Tablet_->GetPhysicalSchema()->GetKeyColumnCount();
        int schemaColumnCount = Tablet_->GetPhysicalSchema()->GetColumnCount();
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
                    for (int j = 0; j < list.GetSize(); ++j) {
                        const auto& dynamicValue = list[j];
                        TVersionedValue versionedValue;
                        static_cast<TUnversionedValue&>(versionedValue) = ToUnversionedValue(dynamicValue.Data, i);
                        versionedValue.Timestamp = Store_->TimestampFromRevision(TSortedDynamicStoreRevision(dynamicValue.Revision));
                        builder.AppendFormat(" %v#%v", i, versionedValue);
                    }
                }
            }
            builder.AppendString("]");

            auto dumpTimestamps = [&] (TRevisionList list) {
                builder.AppendChar('[');
                while (list) {
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

private:
    void CreateDynamicStore() override
    {
        auto config = New<TTabletManagerConfig>();
        config->MaxBlockedRowWaitTime = TDuration::MilliSeconds(100);

        Store_ = New<TSortedDynamicStore>(
            TTabletId(),
            Tablet_.get(),
            StoreContext_);
    }

    IDynamicStorePtr GetDynamicStore() override
    {
        return Store_;
    }

    TUnversionedValue ToUnversionedValue(const TDynamicValueData& data, int index)
    {
        TUnversionedValue value{};
        value.Id = index;
        value.Type = Tablet_->GetPhysicalSchema()->Columns()[index].GetWireType();
        if (IsStringLikeType(value.Type)) {
            value.Length = data.String->Length;
            value.Data.String = data.String->Data;
        } else {
            ::memcpy(&value.Data, &data, sizeof(data));
        }
        return value;
    }

    void LockRow(TTestTransaction* transaction, bool prelock, TSortedDynamicRow row)
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
    void SetUp() override
    {
        TSingleLockSortedDynamicStoreTest::SetUp();

        Transaction_ = StartTransaction();

        auto schema = GetSchema();

        StaticComparer_ = TStaticComparer(*schema);
        LlvmComparer_ = TSortedDynamicRowKeyComparer(NQueryClient::GenerateComparers(schema->GetKeyColumnTypes()));
    }

    TSortedDynamicRow BuildDynamicRow(
        const TUnversionedOwningRow& row)
    {
        auto transaction = StartTransaction();
        auto dynamicRow = WriteRow(transaction.get(), row, false);
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), dynamicRow);
        CommitTransaction(transaction.get());
        WriteRow(transaction.get(), dynamicRow, row);
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

        int operator()(TSortedDynamicRow lhs, TUnversionedValueRange rhs) const
        {
            YT_ASSERT(std::ssize(rhs) >= KeyColumnCount_);
            return Compare(lhs, rhs.Begin(), KeyColumnCount_);
        }

        int operator()(TUnversionedRow lhs, TUnversionedRow rhs) const
        {
            return CompareRows(lhs, rhs);
        }

    private:
        int Compare(TSortedDynamicRow lhs, TSortedDynamicRow rhs) const
        {
            TDynamicTableKeyMask nullKeyBit = 1;
            TDynamicTableKeyMask lhsNullKeyMask = lhs.GetNullKeyMask();
            TDynamicTableKeyMask rhsNullKeyMask = rhs.GetNullKeyMask();
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

                switch (columnIt->GetWireType()) {
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
                        YT_ABORT();
                }
            }
            return 0;
        }

        int Compare(TSortedDynamicRow lhs, const TUnversionedValue* rhsBegin, int rhsLength) const
        {
            TDynamicTableKeyMask nullKeyBit = 1;
            TDynamicTableKeyMask lhsNullKeyMask = lhs.GetNullKeyMask();
            const auto* lhsValue = lhs.BeginKeys();
            const auto* rhsValue = rhsBegin;

            auto columnIt = Schema_.Columns().begin();
            int lhsLength = KeyColumnCount_;
            int minLength = std::min(lhsLength, rhsLength);
            for (int index = 0;
                 index < minLength;
                 ++index, nullKeyBit <<= 1, ++lhsValue, ++rhsValue, ++columnIt)
            {
                auto lhsType = (lhsNullKeyMask & nullKeyBit) ? EValueType::Null : columnIt->GetWireType();
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
                        YT_ABORT();
                }
            }
            return lhsLength - rhsLength;
        }

        int KeyColumnCount_;
        TTableSchema Schema_;
    };

protected:
    TTableSchemaPtr GetSchema() const override
    {
        return New<TTableSchema>(std::vector{
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
    auto str1 = TStringBuf(std::get<0>(GetParam()));
    auto str2 = TStringBuf(std::get<1>(GetParam()));

    auto urow1 = BuildRow(str1, false);
    auto urow2 = BuildRow(str2, false);

    EXPECT_EQ(
        Sign(StaticComparer_(urow1, urow2)),
        Sign(LlvmComparer_(urow1, urow2)))
        << "row1: " << ToString(urow1) << std::endl
        << "row2: " << ToString(urow2);

    int keyColumnCount = GetSchema()->GetKeyColumnCount();

    if (urow1.GetCount() == keyColumnCount && !HasSentinels(urow1) && !HasSentinels(urow2)) {
        auto drow1 = BuildDynamicRow(urow1);
        EXPECT_EQ(
            Sign(StaticComparer_(drow1, ToKeyRef(urow2))),
            Sign(LlvmComparer_(drow1, ToKeyRef(urow2))))
            << "row1: " << ToString(urow1) << std::endl
            << "row2: " << ToString(urow2);

        if (urow2.GetCount() == keyColumnCount) {
            auto drow2 = BuildDynamicRow(urow2);
            EXPECT_EQ(Sign(StaticComparer_(drow1, drow2)), Sign(LlvmComparer_(drow1, drow2)))
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

INSTANTIATE_TEST_SUITE_P(
    CodeGenerationTest,
    TSortedDynamicRowKeyComparerTest,
    ::testing::Combine(comparerTestParams, comparerTestParams));

TEST_F(TSortedDynamicRowKeyComparerTest, DifferentLength)
{
    auto row1 = BuildKey("1");
    auto row2 = BuildKey("1;<\"type\"=\"min\">#");

    EXPECT_EQ(
        Sign(StaticComparer_(row1, row2)),
        Sign(LlvmComparer_(row1, row2)))
        << "row1: " << ToString(row1) << std::endl
        << "row2: " << ToString(row2);
}

////////////////////////////////////////////////////////////////////////////////

class TSingleLockSortedDynamicStoreReserializeConflictTest
    : public TSingleLockSortedDynamicStoreTest
    , public ::testing::WithParamInterface<std::tuple<ELockType, bool>>
{
public:
    struct TParamSerializer
    {
        template <class TParam>
        std::string operator()(const testing::TestParamInfo<TParam>& info) const
        {
            auto lockType = std::get<0>(info.param);
            bool reserialize = std::get<1>(info.param);
            return ToString(lockType) + (reserialize ? "With" : "Without") + "Reserialize";
        }
    };
};

TEST_P(TSingleLockSortedDynamicStoreReserializeConflictTest, Test)
{
    auto lockType = std::get<0>(GetParam());
    auto reserialize = std::get<1>(GetParam());

    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    TLockMask lockMask;
    lockMask.Set(PrimaryLockIndex, lockType);
    auto row = BuildRow("key=1", false);
    auto lockedRow = LockRow(transaction2.get(), BuildRow("key=1", false), true, lockMask);
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction1.get(), BuildRow("key=1;c=test2"), true));

    auto transaction3 = StartTransaction();
    auto transaction4 = StartTransaction();

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), lockedRow);
    CommitTransaction(transaction2.get());
    WriteRow(transaction2.get(), lockedRow, row);
    CommitRow(transaction2.get(), lockedRow);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction3.get(), BuildRow("key=1;c=test3"), true));

    if (reserialize) {
        ReserializeStore();
    }

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction4.get(), BuildRow("key=1;c=test4"), true));
}

INSTANTIATE_TEST_SUITE_P(
    ConflictAfterReserialization,
    TSingleLockSortedDynamicStoreReserializeConflictTest,
    ::testing::Combine(
        ::testing::Values(
            ELockType::Exclusive,
            ELockType::SharedWrite,
            ELockType::SharedStrong),
        ::testing::Values(true, false)),
    TSingleLockSortedDynamicStoreReserializeConflictTest::TParamSerializer());

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSingleLockSortedDynamicStoreTest, Empty)
{
    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, NullTimestamp), nullptr));
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
    ASSERT_EQ(transaction.get(), lock.WriteTransaction);
    ASSERT_TRUE(transaction->LockedRows().empty());

    ConfirmRow(transaction.get(), row);
    ASSERT_EQ(1u, transaction->LockedRows().size());
    ASSERT_TRUE(transaction->LockedRows()[0].Row == row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), nullptr));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    auto ts = CommitTransaction(transaction.get());
    WriteRow(transaction.get(), row, BuildRow(rowString));
    CommitRow(transaction.get(), row);

    ASSERT_FALSE(row.GetDeleteLockFlag());
    ASSERT_EQ(nullptr, lock.WriteTransaction);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts), rowString));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, PrecedingTimestamp(ts)), nullptr));
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
    ASSERT_EQ(transaction.get(), lock.WriteTransaction);
    ASSERT_TRUE(transaction->LockedRows().empty());

    ConfirmRow(transaction.get(), row);
    ASSERT_EQ(1u, transaction->LockedRows().size());
    ASSERT_TRUE(transaction->LockedRows()[0].Row == row);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), rowString));

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    auto ts2 = CommitTransaction(transaction.get());
    DeleteRow(transaction.get(), row);
    CommitRow(transaction.get(), row);

    ASSERT_FALSE(row.GetDeleteLockFlag());
    ASSERT_EQ(nullptr, lock.WriteTransaction);

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

        auto row = BuildRow("key=1;a=" + ToString(i));
        auto dynamicRow = WriteRow(transaction.get(), row, false);

        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), dynamicRow);

        auto ts = CommitTransaction(transaction.get());
        WriteRow(transaction.get(), dynamicRow, row);
        CommitRow(transaction.get(), dynamicRow);

        timestamps.push_back(ts);
    }

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MaxTimestamp), "key=1;a=99"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;a=99"));

    for (int i = 0; i < 100; ++i) {
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, timestamps[i]), std::string("key=1;a=" + ToString(i))));
    }
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

    EXPECT_EQ(nullptr, GetLock(row).WriteTransaction);
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
    DeleteRow(transaction2.get(), row);
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

    auto newRow = BuildRow("key=1;b=3.14");
    auto dynamicRow = WriteRow(transaction2.get(), newRow, false);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), dynamicRow);

    auto ts2 = CommitTransaction(transaction2.get());
    WriteRow(transaction2.get(), dynamicRow, newRow);
    CommitRow(transaction2.get(), dynamicRow);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
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

    auto row = BuildRow("key=1;a=1");
    auto dynamicRow = WriteRow(transaction1.get(), row, true);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow);

    CommitTransaction(transaction1.get());
    WriteRow(transaction1.get(), dynamicRow, row);
    CommitRow(transaction1.get(), dynamicRow);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;a=2"), true));
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadWriteLocks1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto lockedRow = ModifyRow(transaction1.get(), BuildRow("key=1;c=x", false), true);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;c=test2"), true));

    AbortTransaction(transaction1.get());
    AbortRow(transaction1.get(), lockedRow);

    auto ts1 = WriteRow(BuildRow("key=1;c=test3", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;c=test3"));
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto lockedRow = LockRow(transaction1.get(), BuildRow("key=1", false), true, ELockType::SharedWeak);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;c=test2"), true));

    AbortTransaction(transaction1.get());
    AbortRow(transaction1.get(), lockedRow);

    auto ts1 = WriteRow(BuildRow("key=1;c=test3", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;c=test3"));
}

TEST_F(TSingleLockSortedDynamicStoreTest, TwoReadLocks1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    auto transaction3 = StartTransaction();

    auto lockedRow = LockRow(transaction1.get(), BuildRow("key=1", false), true, ELockType::SharedWeak);
    auto lockedRow2 = LockRow(transaction2.get(), BuildRow("key=1", false), true, ELockType::SharedWeak);
    EXPECT_EQ(lockedRow, lockedRow2);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction3.get(), BuildRow("key=1;c=test2"), true));

    AbortTransaction(transaction1.get());
    AbortRow(transaction1.get(), lockedRow);

    AbortTransaction(transaction2.get());
    AbortRow(transaction2.get(), lockedRow2);

    auto ts1 = WriteRow(BuildRow("key=1;c=test3", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;c=test3"));
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadNotBlocked)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), row);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (
        TSortedDynamicRow /*row*/,
        TSortedDynamicStore::TConflictInfo /*conflictInfo*/,
        TDuration /*timeout*/)
    {
        blocked = true;
        return TSortedDynamicStore::TRowBlockedWaitingResult();
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
    Store_->SetRowBlockedHandler(BIND([&] (
        TSortedDynamicRow blockedRow,
        TSortedDynamicStore::TConflictInfo conflictInfo,
        TDuration /*timeout*/)
    {
        EXPECT_EQ(PrimaryLockIndex, conflictInfo.LockIndex);
        EXPECT_EQ(blockedRow, row);
        AbortTransaction(transaction.get());
        AbortRow(transaction.get(), row);
        blocked = true;
        return TSortedDynamicStore::TRowBlockedWaitingResult();
    }));

    // Blocked, old value is read.
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
    EXPECT_TRUE(blocked);
}

TEST_F(TSingleLockSortedDynamicStoreTest, ReadBlockedCommit)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row = BuildRow("key=1;a=1");
    auto dynamicRow = WriteRow(transaction.get(), BuildRow("key=1;a=1"), false);

    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), dynamicRow);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (
        TSortedDynamicRow blockedRow,
        TSortedDynamicStore::TConflictInfo conflictInfo,
        TDuration /*timeout*/)
    {
        EXPECT_EQ(PrimaryLockIndex, conflictInfo.LockIndex);
        EXPECT_EQ(blockedRow, dynamicRow);
        CommitTransaction(transaction.get());
        WriteRow(transaction.get(), dynamicRow, row);
        CommitRow(transaction.get(), dynamicRow);
        blocked = true;
        return TSortedDynamicStore::TRowBlockedWaitingResult();
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
    Store_->SetRowBlockedHandler(BIND([&] (
        TSortedDynamicRow /*blockedRow*/,
        TSortedDynamicStore::TConflictInfo /*conflictInfo*/,
        TDuration /*timeout*/)
    {
        blocked = true;
        Sleep(TDuration::MilliSeconds(10));
        return TSortedDynamicStore::TRowBlockedWaitingResult();
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
    auto context = transaction2->CreateWriteContext();
    context.Phase = EWritePhase::Prelock;
    TLockMask lockMask;
    lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
    EXPECT_EQ(TSortedDynamicRow(), Store_->ModifyRow(row, lockMask, false, &context));
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
    auto context = transaction2->CreateWriteContext();
    context.Phase = EWritePhase::Prelock;
    TLockMask lockMask;
    lockMask.Set(PrimaryLockIndex, ELockType::Exclusive);
    EXPECT_EQ(TSortedDynamicRow(), Store_->ModifyRow(row, lockMask, false, &context));
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
        ChunkReadOptions_,
        /*workloadCategory*/ std::nullopt);

    EXPECT_TRUE(WaitForFast(reader->Open()).IsOK());

    std::vector<TVersionedRow> rows;
    rows.reserve(10);

    TRowBatchReadOptions options{
        .MaxRowsPerRead = 10
    };

    auto batch = reader->Read(options);
    EXPECT_TRUE(batch);
    EXPECT_EQ(1, batch->GetRowCount());

    EXPECT_FALSE(reader->Read(options));
}

TEST_F(TSingleLockSortedDynamicStoreTest, SerializeEmpty)
{
    auto check = [&] {
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

    auto check = [&] {
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

    auto check = [&] {
        EXPECT_EQ(1, Store_->GetRowCount());
        EXPECT_EQ(2, Store_->GetValueCount());
        EXPECT_EQ(ts1, Store_->GetMinTimestamp());
        EXPECT_EQ(ts3, Store_->GetMaxTimestamp());

        EXPECT_TRUE(AreRowsEqual(LookupRow(key, PrecedingTimestamp(ts1)), nullptr));
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

    auto expectedDump =
        Format("RowCount=1 ValueCount=1 MinTimestamp=%v MaxTimestamp=%v\n", ts1, ts1) +
        Format("[ 0#1 ] -> [ 1#1#1@%x] wts#0: [ %v ] dts: [ ]\n", ts1, ts1);

    EXPECT_EQ(expectedDump, DumpStore());

    WriteRow(BuildRow("key=2;a=2", false));

    EXPECT_EQ(2, Store_->GetRowCount());
    EXPECT_EQ(2, Store_->GetValueCount());

    EndReserializeStore(snapshot);

    // Values written after serialization are not saved and restored.
    auto expectedDump2 =
        Format("RowCount=1 ValueCount=1 MinTimestamp=%v MaxTimestamp=%v\n", ts1, ts1) +
        Format("[ 0#1 ] -> [ 1#1#1@%x] wts#0: [ %v ] dts: [ ]\n", ts1, ts1);

    EXPECT_EQ(expectedDump2, DumpStore());

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(1, Store_->GetValueCount());
    EXPECT_EQ(ts1, Store_->GetMinTimestamp());
    EXPECT_EQ(ts1, Store_->GetMaxTimestamp());

    auto key1 = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key1, PrecedingTimestamp(ts1)), nullptr));
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
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, PrecedingTimestamp(ts1)), nullptr));
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

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, PrecedingTimestamp(ts1)), nullptr));
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
    auto row1 = BuildRow("key=1;b=2.7", false);
    auto dynamicRow1 = WriteRow(transaction1.get(), row1, false);
    auto row2 = BuildRow("key=2;b=3.1", false);
    auto dynamicRow2 = WriteRow(transaction1.get(), row2, false);
    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow1);
    PrepareRow(transaction1.get(), dynamicRow2);
    CommitTransaction(transaction1.get());
    WriteRow(transaction1.get(), dynamicRow1, row1);
    CommitRow(transaction1.get(), dynamicRow1);
    WriteRow(transaction1.get(), dynamicRow2, row2);
    CommitRow(transaction1.get(), dynamicRow2);

    auto transaction2 = StartTransaction();
    auto row1_ = BuildRow("key=1;a=1", false);
    auto dynamicRow1_ = WriteRow(transaction2.get(), row1_, false);
    EXPECT_EQ(dynamicRow1, dynamicRow1_);
    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), dynamicRow1);
    CommitTransaction(transaction2.get());
    WriteRow(transaction2.get(), dynamicRow1, row1_);
    CommitRow(transaction2.get(), dynamicRow1);

    auto dump = DumpStore();
    ReserializeStore();
    EXPECT_EQ(dump, DumpStore());
}

////////////////////////////////////////////////////////////////////////////////

class TMultiLockSortedDynamicStoreTest
    : public TSingleLockSortedDynamicStoreTest
{
protected:
    TTableSchemaPtr GetSchema() const override
    {
        // NB: Key columns must go first.
        return New<TTableSchema>(std::vector{
            TColumnSchema(TColumnSchema("key", EValueType::Int64).SetSortOrder(ESortOrder::Ascending)),
            TColumnSchema(TColumnSchema("a", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("b", EValueType::Double).SetLock("l2")),
            TColumnSchema(TColumnSchema("c", EValueType::String))
        });
    }

    TLockMask LockMask1;
    TLockMask LockMask2;
    TLockMask LockMask12;

    void SetUp() override
    {
        TSingleLockSortedDynamicStoreTest::SetUp();
        LockMask1.Set(1, ELockType::SharedWeak);
        LockMask2.Set(2, ELockType::SharedWeak);

        LockMask12 = MaxMask(LockMask1, LockMask2);
    }

};

TEST_F(TMultiLockSortedDynamicStoreTest, TwoReadLocksWriteConflict)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    auto transaction3 = StartTransaction();

    auto lockedRow1 = LockRow(transaction1.get(), BuildRow("key=1", false), true, LockMask1);
    auto lockedRow2 = LockRow(transaction2.get(), BuildRow("key=1", false), true, LockMask2);
    EXPECT_EQ(lockedRow1, lockedRow2);
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction3.get(), BuildRow("key=1;c=test2"), true));

    AbortTransaction(transaction1.get());
    AbortRow(transaction1.get(), lockedRow1);

    AbortTransaction(transaction2.get());
    AbortRow(transaction2.get(), lockedRow2);

    auto ts1 = WriteRow(BuildRow("key=1;c=test3", false));

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;c=test3"));
}

TEST_F(TMultiLockSortedDynamicStoreTest, ReadWriteWriteConflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    auto lockedRow = LockRow(transaction2.get(), BuildRow("key=1", false), true, LockMask1);
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction1.get(), BuildRow("key=1;c=test2"), true, LockMask1));
    auto writtenRow = WriteRow(transaction1.get(), BuildRow("key=1;b=1.0", false), true, LockMask2);

    auto transaction3 = StartTransaction();

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), lockedRow);
    CommitTransaction(transaction2.get());
    CommitRow(transaction2.get(), lockedRow);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), writtenRow);
    CommitTransaction(transaction1.get());
    CommitRow(transaction1.get(), writtenRow);

    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction3.get(), BuildRow("key=1;c=test3"), true, LockMask2));
}

TEST_F(TMultiLockSortedDynamicStoreTest, ConcurrentWrites1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto row = BuildRow("key=1;a=1", false);
    auto dynamicRow = WriteRow(transaction1.get(), row, true, LockMask1);

    auto transaction2 = StartTransaction();
    auto row2 = BuildRow("key=1;b=3.14", false);
    auto dynamicRow2 = WriteRow(transaction2.get(), row2, true, LockMask2);
    EXPECT_EQ(dynamicRow, dynamicRow2);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow);

    auto ts1 = CommitTransaction(transaction1.get());
    WriteRow(transaction1.get(), dynamicRow, row);
    CommitRow(transaction1.get(), dynamicRow);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(dynamicRow, 1));

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), dynamicRow2);

    auto ts2 = CommitTransaction(transaction2.get());
    WriteRow(transaction2.get(), dynamicRow2, row2);
    CommitRow(transaction2.get(), dynamicRow2);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(dynamicRow, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(dynamicRow, 2));
}

TEST_F(TMultiLockSortedDynamicStoreTest, ConcurrentWrites2)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();

    auto transaction2 = StartTransaction();
    auto row = BuildRow("key=1;b=3.14", false);
    auto dynamicRow = WriteRow(transaction2.get(), row, true, LockMask2);

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), dynamicRow);

    auto ts2 = CommitTransaction(transaction2.get());
    WriteRow(transaction2.get(), dynamicRow, row);
    CommitRow(transaction2.get(), dynamicRow);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(dynamicRow, 2));

    auto row1 = BuildRow("key=1;a=1", false);
    auto dynamicRow1 = WriteRow(transaction1.get(), row1, true, LockMask1);
    EXPECT_EQ(dynamicRow, dynamicRow1);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow1);

    auto ts1 = CommitTransaction(transaction1.get());
    WriteRow(transaction1.get(), dynamicRow1, row1);
    CommitRow(transaction1.get(), dynamicRow1);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(dynamicRow, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(dynamicRow, 2));
}

TEST_F(TMultiLockSortedDynamicStoreTest, ConcurrentWrites3)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row1 = BuildRow("key=1;b=3.14", false);
    auto dynamicRow1 = WriteRow(transaction1.get(), row1, true, LockMask2);
    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow1);

    auto row2 = BuildRow("key=1;a=1", false);
    auto dynamicRow2 = WriteRow(transaction2.get(), row2, true, LockMask1);
    EXPECT_EQ(dynamicRow1, dynamicRow2);
    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), dynamicRow2);

    AbortTransaction(transaction1.get());
    AbortRow(transaction1.get(), dynamicRow1);

    auto ts2 = CommitTransaction(transaction2.get());
    WriteRow(transaction2.get(), dynamicRow2, row2);
    CommitRow(transaction2.get(), dynamicRow2);

    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;a=1"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow2));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(dynamicRow2, 1));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow2, 2));
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
    WriteRow(transaction1.get(), BuildRow("key=1;a=1;b=3.14", false), true, LockMask12);

    auto transaction2 = StartTransaction();
    EXPECT_EQ(TSortedDynamicRow(), WriteRow(transaction2.get(), BuildRow("key=1;a=2", false), true, LockMask1));
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteWriteConflict3)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;c=test", false), true);

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

TEST_F(TMultiLockSortedDynamicStoreTest, UnpreparedSharedWriterDoesNotBlockReads)
{
    auto key = BuildKey("1");
    WriteRow(BuildRow("key=1;a=1", false));

    TLockMask sharedWriteMask;
    sharedWriteMask.Set(1, ELockType::SharedWrite);
    auto sharedWriter = StartTransaction();
    auto sharedRow = LockRow(sharedWriter.get(), key, false, sharedWriteMask);

    EXPECT_NO_THROW({
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), "key=1;a=1"));
    });

    auto otherWriter = StartTransaction();
    auto otherRow = WriteRow(otherWriter.get(), BuildRow("key=1;b=2.0", false), false, LockMask2);
    PrepareTransaction(otherWriter.get());

    // Preparing another lock recalculates prepare timestamps for the whole row.
    PrepareRow(otherWriter.get(), otherRow);

    EXPECT_EQ(NullTimestamp, sharedWriter->GetPrepareTimestamp());
    EXPECT_EQ(NotPreparedTimestamp, GetLock(sharedRow, 1).PrepareTimestamp.load());

    AbortTransaction(otherWriter.get());
    AbortRow(otherWriter.get(), otherRow);

    EXPECT_NO_THROW({
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), "key=1;a=1"));
    });

    AbortTransaction(sharedWriter.get());
    AbortRow(sharedWriter.get(), sharedRow);
}

TEST_F(TMultiLockSortedDynamicStoreTest, WriteNotBlocked)
{
    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row1 = WriteRow(transaction1.get(), BuildRow("key=1;a=1", false), false, LockMask1);

    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), row1);

    bool blocked = false;
    Store_->SetRowBlockedHandler(BIND([&] (
        TSortedDynamicRow /*blockedRow*/,
        TSortedDynamicStore::TConflictInfo /*conflictInfo*/,
        TDuration /*timeout*/)
    {
        blocked = true;
        return TSortedDynamicStore::TRowBlockedWaitingResult();
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

    auto row1 = BuildRow("key=1;a=1", false);
    auto dynamicRow1 = WriteRow(transaction1.get(), row1, false, LockMask1);
    auto row2 = BuildRow("key=1;b=3.14", false);
    auto dynamicRow2 = WriteRow(transaction2.get(), row2, false, LockMask2);
    EXPECT_EQ(dynamicRow1, dynamicRow2);
    auto dynamicRow = dynamicRow1;

    // Mind the order!
    PrepareTransaction(transaction1.get());
    PrepareRow(transaction1.get(), dynamicRow1);

    PrepareTransaction(transaction2.get());
    PrepareRow(transaction2.get(), dynamicRow2);

    auto ts2 = CommitTransaction(transaction2.get());
    WriteRow(transaction2.get(), dynamicRow2, row2);
    CommitRow(transaction2.get(), dynamicRow2);

    auto ts1 = CommitTransaction(transaction1.get());
    WriteRow(transaction1.get(), dynamicRow1, row1);
    CommitRow(transaction1.get(), dynamicRow1);

    EXPECT_LE(ts2, ts1);

    auto key = BuildKey("1");
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, MinTimestamp), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts1), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, ts2), "key=1;b=3.14"));
    EXPECT_EQ(MinTimestamp, GetLastCommitTimestamp(dynamicRow));
    EXPECT_EQ(ts1, GetLastCommitTimestamp(dynamicRow, 1));
    EXPECT_EQ(ts2, GetLastCommitTimestamp(dynamicRow, 2));

    {
        auto reader = Store_->CreateSnapshotReader();
        WaitForFast(reader->Open())
            .ThrowOnError();

        TRowBatchReadOptions options{
            .MaxRowsPerRead = 1
        };
        auto batch = reader->Read(options);
        EXPECT_TRUE(batch);
        EXPECT_EQ(1, batch->GetRowCount());

        auto row = batch->MaterializeRows()[0];
        EXPECT_EQ(1, row.GetKeyCount());
        EXPECT_EQ(2, row.GetValueCount());
        EXPECT_EQ(2, row.GetWriteTimestampCount());
        EXPECT_EQ(ts1, row.WriteTimestamps()[0]);
        EXPECT_EQ(ts2, row.WriteTimestamps()[1]);
        EXPECT_EQ(0, row.GetDeleteTimestampCount());

        EXPECT_FALSE(reader->Read(options));
    }
}

TEST_F(TMultiLockSortedDynamicStoreTest, SerializeSnapshot1)
{
    auto key = BuildKey("1");

    auto ts1 = DeleteRow(key);
    auto ts2 = WriteRow(BuildRow("key=1;a=1", false), LockMask1);
    auto ts3 = WriteRow(BuildRow("key=1;c=test", false));
    auto ts4 = WriteRow(BuildRow("key=1;b=3.14", false), LockMask2);

    auto check = [&] {
        EXPECT_EQ(1, Store_->GetRowCount());
        EXPECT_EQ(3, Store_->GetValueCount());

        auto row = LookupDynamicRow(key);
        EXPECT_EQ(ts3, GetLastCommitTimestamp(row));
        EXPECT_EQ(ts3, GetLastCommitTimestamp(row, 1));
        EXPECT_EQ(ts4, GetLastCommitTimestamp(row, 2));

        EXPECT_TRUE(AreRowsEqual(LookupRow(key, PrecedingTimestamp(ts1)), nullptr));
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

// Rows containing only nulls exist; pure locks never establish existence.
class TSortedLockingTest
    : public TSingleLockSortedDynamicStoreTest
{
public:
    struct TLockHistoryCase
    {
        const char* Name = nullptr;
        // nullptr: no committed write; "": all values null.
        const char* Values = nullptr;
        bool Delete = false;
        bool Exists = false;
    };

    static constexpr TLockHistoryCase LockHistories[] = {
        // Name     Initial values             Delete  Exists
        {"Missing", nullptr,                   false,  false},
        {"Live",    "a=1;b=3.14;c=old;peer=9", false,  true },
        {"Nulls",   "",                        false,  true },
        {"Deleted", "a=1;b=3.14;c=old;peer=9", true,   false},
    };

protected:
    using TGroupMask = std::bitset<3>;
    using TWriterMask = ui32;

    enum class ELockMutation
    {
        Write,
        Delete,
        Lock,
    };

    struct TLockOperation
    {
        TGroupMask Groups;
        std::string Values;
        ELockType LockType = ELockType::Exclusive;
        ELockMutation Kind = ELockMutation::Write;
    };

    struct TLockMutationCase
    {
        const char* Name = nullptr;
        // Named T1, T2, ... in diagnostics.
        std::vector<TLockOperation> Writers;
    };

    struct TLockProjectionCase
    {
        const char* Name = nullptr;
        TColumnFilter Filter;
    };

    enum class EOwnerState
    {
        Active,
        Prepared,
        Committed,
        Aborted,
    };

    // Owner acquired its locks first; requester is another transaction asking for
    // locks on the same key, not a snapshot reader. Each timing case specifies the
    // owner's state at that request and the requester's relative start timestamp.
    struct TLockTimingCase
    {
        const char* Name = nullptr;
        EOwnerState OwnerState = EOwnerState::Active;
        int RequesterStart = 0;
    };

    enum class EMutationOutcome
    {
        Allowed,
        Wait,
        Conflict,
    };

    struct TPendingLockWait
    {
        TSortedDynamicRow Row;
        TLockMask LockMask;
        TTimestamp Timestamp;
    };

    struct TLockTransaction
    {
        TLockOperation Operation;
        TLegacyOwningKey Key;
        TUnversionedOwningRow Data;
        TTimestamp Commit = NullTimestamp;
        std::unique_ptr<TTestTransaction> Transaction;
        TSortedDynamicRowWithLock Row = TSortedDynamicRow();
        std::optional<TPendingLockWait> PendingWait;
        bool Finished = false;
    };

    static constexpr int KeyColumnId = 0;
    static constexpr int ColumnCount = 5;
    static constexpr int ColumnGroups[] = {0, 1, 2, 0, 1}; // key, a, b, c, peer.
    static constexpr TStringBuf GroupNames[] = {"primary", "a/peer", "b"};

    inline static const TGroupMask PrimaryGroup{0b001};
    inline static const TGroupMask GroupA{0b010};
    inline static const TGroupMask GroupB{0b100};
    inline static const TGroupMask AllGroups{0b111};

    // Lock modes apply to groups; acquiring a lock alone does not change values.
    // - Exclusive: write mode, incompatible with every other mode.
    // - SharedWrite: write mode compatible with other SharedWrite owners.
    // - SharedStrong: read-only mode compatible with other read locks; commit records
    //   a read timestamp that conflicts with writers whose start timestamp is earlier.
    // - SharedWeak: read-only mode with the same compatibility while pending,
    //   but no conflict timestamp remains after commit.
    static constexpr ELockType ConflictLockTypes[] = {
        ELockType::Exclusive,
        ELockType::SharedWrite,
        ELockType::SharedStrong,
        ELockType::SharedWeak,
    };

    // Owner is active or prepared and still holds its locks. True means a conflict
    // on overlapping groups; rows and columns follow ConflictLockTypes.
    static constexpr bool PendingTransactionLockConflict[4][4] = {
        // Owner / requester: Exclusive  SharedWrite  SharedStrong  SharedWeak
        /* Exclusive    */   {true,      true,        true,         true },
        /* SharedWrite  */   {true,      false,       true,         true },
        /* SharedStrong */   {true,      true,        false,        false},
        /* SharedWeak   */   {true,      true,        false,        false},
    };

    // Owner has committed and released its locks. Recorded timestamps still cause
    // conflicts if the owner's commit is after the requester's start timestamp.
    static constexpr bool CommittedTransactionLockConflict[4][4] = {
        // Owner / requester: Exclusive  SharedWrite  SharedStrong  SharedWeak
        /* Exclusive    */   {true,      true,        true,         true },
        /* SharedWrite  */   {true,      false,       true,         true },
        /* SharedStrong */   {true,      true,        false,        false},
        /* SharedWeak   */   {false,     false,       false,        false},
    };

    static constexpr int OwnerStart = 10;
    static constexpr int OwnerPrepare = 20;
    static constexpr int OwnerCommit = 30;
    static constexpr int CaseTimestampStride = 100;

    // Before/at/after compares the owner's event to the requester's start timestamp.
    static constexpr TLockTimingCase LockTimings[] = {
        // Name                   Owner state               Requester start
        {"Active",                EOwnerState::Active,      OwnerCommit + 1},
        {"PreparedBeforeStart",   EOwnerState::Prepared,    OwnerPrepare + 1},
        {"PreparedAtStart",       EOwnerState::Prepared,    OwnerPrepare},
        {"PreparedAfterStart",    EOwnerState::Prepared,    OwnerPrepare - 1},
        {"CommittedBeforeStart",  EOwnerState::Committed,   OwnerCommit + 1},
        {"CommittedAtStart",      EOwnerState::Committed,   OwnerCommit},
        {"CommittedAfterStart",   EOwnerState::Committed,   OwnerCommit - 1},
        {"Aborted",               EOwnerState::Aborted,     OwnerCommit + 1},
    };

    // Representative group relationships in both directions. All seven masks occur;
    // deletes additionally exercise every owner/requester mask in the test below.
    const std::vector<std::pair<TGroupMask, TGroupMask>> MutationGroupPairs_ = {
        // Owner groups          Requester groups
        {PrimaryGroup,           PrimaryGroup},
        {GroupA,                 GroupA},
        {GroupB,                 GroupB},
        {GroupA,                 GroupB},
        {GroupB,                 GroupA},
        {PrimaryGroup,           GroupA},
        {GroupA,                 PrimaryGroup},
        {GroupA | GroupB,        GroupA},
        {GroupA,                 GroupA | GroupB},
        {GroupA | GroupB,        GroupA | GroupB},
        {PrimaryGroup | GroupA,  GroupB},
        {GroupB,                 PrimaryGroup | GroupA},
        {PrimaryGroup | GroupB,  GroupA},
        {GroupA,                 PrimaryGroup | GroupB},
        {AllGroups,              GroupA | GroupB},
        {GroupA | GroupB,        AllGroups},
    };

    const std::vector<TLockMutationCase> LockMutations_ = {
        // Name                    Writers: {groups, values, lock type = Exclusive, kind = Write}
        {"WriteA",                 {{GroupA, "a=2"}}},
        {"WritePeer",              {{GroupA, "peer=8"}}},
        {"WriteB",                 {{GroupB, "b=6.28"}}},
        {"WritePrimary",           {{PrimaryGroup, "c=new"}}},
        {"WriteBoth",              {{GroupA | GroupB, "a=2;b=6.28"}}},
        {"WriteNull",              {{GroupA, "a=#"}}},
        {"SharedWriteA",           {{GroupA, "a=2", ELockType::SharedWrite}}},
        {"SharedWriteB",           {{GroupB, "b=6.28", ELockType::SharedWrite}}},
        {"SharedWritePrimary",     {{PrimaryGroup, "c=new", ELockType::SharedWrite}}},
        {"TwoSharedWrites", {
            {GroupB, "b=6.28", ELockType::SharedWrite},
            {GroupB, "b=6.28", ELockType::SharedWrite},
        }},
        {"IndependentWrites",      {{GroupA, "a=2"}, {GroupB, "b=6.28"}}},
        {"IndependentNullWrite",   {{GroupA, "a=#"}, {GroupB, "b=6.28"}}},
        {"Delete",                 {{PrimaryGroup, "", ELockType::Exclusive, ELockMutation::Delete}}},
        {"WeakLock",               {{GroupA, "", ELockType::SharedWeak, ELockMutation::Lock}}},
        {"StrongLock",             {{GroupA, "", ELockType::SharedStrong, ELockMutation::Lock}}},
        {"ExclusiveLock",          {{GroupA, "", ELockType::Exclusive, ELockMutation::Lock}}},
        {"SharedWriteLock",        {{GroupA, "", ELockType::SharedWrite, ELockMutation::Lock}}},
    };

    // All eight subsets of value groups, plus a same-group peer and a universal filter.
    // Column IDs: key = 0, a = 1, b = 2, c = 3, peer = 4.
    const std::vector<TLockProjectionCase> LockProjections_ = {
        // Name        Columns
        {"Keys",       TColumnFilter({0})},
        {"A",          TColumnFilter({0, 1})},
        {"B",          TColumnFilter({0, 2})},
        {"Primary",    TColumnFilter({0, 3})},
        {"AB",         TColumnFilter({0, 1, 2})},
        {"APrimary",   TColumnFilter({0, 1, 3})},
        {"BPrimary",   TColumnFilter({0, 2, 3})},
        {"ABPrimary",  TColumnFilter({0, 1, 2, 3})},
        {"Peer",       TColumnFilter({0, 4})},
        {"Universal",  TColumnFilter::MakeUniversal()},
    };

    static bool IsWritingLock(ELockType lockType)
    {
        return lockType == ELockType::Exclusive || lockType == ELockType::SharedWrite;
    }

    static bool HaveOverlappingGroups(TGroupMask lhs, TGroupMask rhs)
    {
        // Primary locks cover the whole row, including groups absent from the mask.
        return lhs.test(PrimaryLockIndex) || rhs.test(PrimaryLockIndex) || (lhs & rhs).any();
    }

    static TLockMask MakeLockMask(const TLockOperation& operation)
    {
        TLockMask lockMask;
        for (int index = 0; index < std::ssize(GroupNames); ++index) {
            if (operation.Groups.test(index)) {
                lockMask.Set(index, operation.LockType);
            }
        }

        return lockMask;
    }

    static std::vector<TStringBuf> DescribeGroups(TGroupMask groups)
    {
        std::vector<TStringBuf> names;
        for (int index = 0; index < std::ssize(GroupNames); ++index) {
            if (groups.test(index)) {
                names.push_back(GroupNames[index]);
            }
        }

        return names;
    }

    static TGroupMask ComputeProjectionGroups(const TColumnFilter& columnFilter)
    {
        // Always check primary for deletes; other groups follow the test schema.
        auto groups = PrimaryGroup;
        for (int column = KeyColumnId + 1; column < ColumnCount; ++column) {
            if (columnFilter.ContainsIndex(column)) {
                groups.set(ColumnGroups[column]);
            }
        }

        return groups;
    }

    static EMutationOutcome ComputeExpectedMutationOutcome(
        const TLockOperation& owner,
        const TLockOperation& requester,
        const TLockTimingCase& timing)
    {
        if (!HaveOverlappingGroups(owner.Groups, requester.Groups) || timing.OwnerState == EOwnerState::Aborted) {
            return EMutationOutcome::Allowed;
        }

        auto lockIndex = [] (ELockType type) {
            auto it = std::find(std::begin(ConflictLockTypes), std::end(ConflictLockTypes), type);
            YT_VERIFY(it != std::end(ConflictLockTypes));

            return it - std::begin(ConflictLockTypes);
        };

        auto ownerIndex = lockIndex(owner.LockType);
        auto requesterIndex = lockIndex(requester.LockType);
        bool conflict = timing.OwnerState == EOwnerState::Committed
            ? timing.RequesterStart < OwnerCommit && CommittedTransactionLockConflict[ownerIndex][requesterIndex]
            : PendingTransactionLockConflict[ownerIndex][requesterIndex];

        if (!conflict) {
            return EMutationOutcome::Allowed;
        }

        return timing.OwnerState == EOwnerState::Prepared && timing.RequesterStart > OwnerPrepare && IsWritingLock(owner.LockType)
            ? EMutationOutcome::Wait
            : EMutationOutcome::Conflict;
    }

    TTableSchemaPtr GetSchema() const override
    {
        return New<TTableSchema>(std::vector{
            TColumnSchema("key", EValueType::Int64).SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64).SetLock("a"),
            TColumnSchema("b", EValueType::Double).SetLock("b"),
            TColumnSchema("c", EValueType::String),
            TColumnSchema("peer", EValueType::Int64).SetLock("a"),
        });
    }

    TLockTransaction StartLockTransaction(
        int keyValue,
        const TLockOperation& operation,
        TTimestamp startTimestamp = NullTimestamp)
    {
        return {
            .Operation = operation,
            .Key = BuildKey(ToString(keyValue)),
            .Data = BuildRow(Format("key=%v;%v", keyValue, operation.Values), /*treatMissingAsNull*/ false),
            .Transaction = StartTransaction(startTimestamp),
        };
    }

    // Set up an uncontended owner in the confirmed lock phase.
    bool AcquireLocks(TLockTransaction* writer)
    {
        writer->Row = writer->Operation.Kind == ELockMutation::Delete
            ? DeleteRow(writer->Transaction.get(), writer->Key, /*prelock*/ false)
            : LockRow(writer->Transaction.get(), writer->Key, /*prelock*/ false, MakeLockMask(writer->Operation));

        return static_cast<bool>(writer->Row);
    }

    void PrepareLocks(TLockTransaction* writer, TTimestamp timestamp)
    {
        PrepareTransaction(writer->Transaction.get(), timestamp);
        PrepareRow(writer->Transaction.get(), writer->Row);
    }

    void AbortIfUnfinished(TLockTransaction* writer)
    {
        if (writer->Transaction && !writer->Finished) {
            FinishLockTransaction(writer, /*commit*/ false);
        }
    }

    void FinishLockTransaction(TLockTransaction* writer, bool commit)
    {
        YT_VERIFY(!writer->Finished);
        auto* transaction = writer->Transaction.get();
        if (commit) {
            CommitTransaction(transaction, writer->Commit);
            if (writer->Operation.Kind == ELockMutation::Delete) {
                DeleteRow(transaction, writer->Row);
            } else if (writer->Operation.Kind == ELockMutation::Write) {
                WriteRow(transaction, writer->Row, writer->Data);
            }

            CommitRow(transaction, writer->Row, writer->Operation.Kind == ELockMutation::Lock);
        } else {
            AbortTransaction(transaction);
            if (writer->Row) {
                AbortRow(transaction, writer->Row);
            }
        }

        writer->Finished = true;
    }

    EMutationOutcome RequestLocks(TLockTransaction* requester)
    {
        auto context = requester->Transaction->CreateWriteContext();
        context.Phase = EWritePhase::Prelock;
        auto lockMask = MakeLockMask(requester->Operation);
        requester->Row = TSortedDynamicRowWithLock(
            Store_->ModifyRow(requester->Key, lockMask, requester->Operation.Kind == ELockMutation::Delete, &context),
            lockMask);
        requester->PendingWait.reset();

        auto outcome = requester->Row
            ? EMutationOutcome::Allowed
            : (context.BlockedStore ? EMutationOutcome::Wait : EMutationOutcome::Conflict);

        EXPECT_EQ(outcome != EMutationOutcome::Conflict, context.Error.IsOK());

        if (context.BlockedStore) {
            EXPECT_EQ(Store_.Get(), context.BlockedStore);
            requester->PendingWait = TPendingLockWait{
                .Row = context.BlockedRow,
                .LockMask = context.BlockedLockMask,
                .Timestamp = context.BlockedTimestamp,
            };
        }

        return outcome;
    }

    // Complete exactly one registered wait by aborting the owner transaction.
    void CompleteWaitAndAbortOwnerTransaction(TLockTransaction* requester, TLockTransaction* owner)
    {
        ASSERT_TRUE(requester->PendingWait);
        int waits = 0;
        auto handlerGuard = Finally([&] {
            Store_->ResetRowBlockedHandler();
        });
        Store_->SetRowBlockedHandler(BIND([&] (
            TSortedDynamicRow /*row*/,
            TSortedDynamicStore::TConflictInfo /*conflict*/,
            TDuration /*timeout*/)
        {
            ++waits;
            THROW_ERROR_EXCEPTION_IF(owner->Finished, "Wait continued after the owner transaction was aborted");

            FinishLockTransaction(owner, /*commit*/ false);

            return TSortedDynamicStore::TRowBlockedWaitingResult();
        }));

        const auto& pending = *requester->PendingWait;
        Store_->WaitOnBlockedRow(pending.Row, pending.LockMask, pending.Timestamp);
        requester->PendingWait.reset();
        EXPECT_EQ(1, waits);
    }

    static TGroupMask FindUnrelatedGroup(const TLockOperation& owner, const TLockOperation& requester)
    {
        auto usedGroups = owner.Groups | requester.Groups;
        if (usedGroups.test(PrimaryLockIndex)) {
            return {};
        }

        auto freeGroups = (GroupA | GroupB) & ~usedGroups;
        return (freeGroups & GroupA).any() ? GroupA : freeGroups & GroupB;
    }

    void RunMutationCase(
        int keyValue,
        const TLockOperation& owner,
        const TLockOperation& requester,
        const TLockTimingCase& timing)
    {
        SCOPED_TRACE(Format(
            "Owner=%v groups=%v delete=%v Requester=%v groups=%v delete=%v Timing=%v",
            owner.LockType,
            DescribeGroups(owner.Groups),
            owner.Kind == ELockMutation::Delete,
            requester.LockType,
            DescribeGroups(requester.Groups),
            requester.Kind == ELockMutation::Delete,
            timing.Name));

        // Cases share one store. Give each a disjoint timestamp interval while
        // preserving the relative start/prepare/commit boundaries from the table.
        auto adjustTimestamp = [&] (int offset) {
            return TTimestamp(CaseTimestampStride * keyValue + offset);
        };

        auto ownerTransaction = StartLockTransaction(keyValue, owner, adjustTimestamp(OwnerStart));
        ownerTransaction.Commit = adjustTimestamp(OwnerCommit);
        TLockTransaction requesterTransaction{.Operation = requester};
        TLockTransaction unrelatedTransaction;

        // Also covers the optional unrelated transaction assigned below.
        auto transactionsGuard = Finally([&] {
            for (auto* transaction : {&requesterTransaction, &unrelatedTransaction, &ownerTransaction}) {
                AbortIfUnfinished(transaction);
            }
        });

        // 1. The owner acquires locks, then reaches the state described by timing.
        ASSERT_TRUE(AcquireLocks(&ownerTransaction));
        if (timing.OwnerState != EOwnerState::Active) {
            PrepareLocks(&ownerTransaction, adjustTimestamp(OwnerPrepare));
        }

        if (timing.OwnerState == EOwnerState::Committed || timing.OwnerState == EOwnerState::Aborted) {
            FinishLockTransaction(&ownerTransaction, timing.OwnerState == EOwnerState::Committed);
        }

        // 2. An unrelated prepared lock must not block the request or its retry.
        auto unrelatedGroup = FindUnrelatedGroup(owner, requester);
        if (unrelatedGroup.any()) {
            unrelatedTransaction = StartLockTransaction(
                keyValue,
                {.Groups = unrelatedGroup, .Kind = ELockMutation::Lock},
                adjustTimestamp(OwnerStart));
            ASSERT_TRUE(AcquireLocks(&unrelatedTransaction));
            PrepareLocks(&unrelatedTransaction, adjustTimestamp(OwnerPrepare));
        }

        // 3. The requester asks for locks; compare the result with the independent model.
        requesterTransaction = StartLockTransaction(keyValue, requester, adjustTimestamp(timing.RequesterStart));
        auto expected = ComputeExpectedMutationOutcome(owner, requester, timing);
        auto actual = RequestLocks(&requesterTransaction);
        EXPECT_EQ(expected, actual);

        // 4. A waiting request must succeed after its owner is aborted, even while
        // the unrelated transaction remains prepared.
        if (actual == EMutationOutcome::Wait) {
            ASSERT_NO_THROW(CompleteWaitAndAbortOwnerTransaction(&requesterTransaction, &ownerTransaction));
            EXPECT_EQ(EMutationOutcome::Allowed, RequestLocks(&requesterTransaction));
        }
    }
};

TEST_F(TSortedLockingTest, MutationCompatibility)
{
    std::vector<TLockOperation> operations;
    for (auto lockType : ConflictLockTypes) {
        for (ui32 bits = 1; bits <= AllGroups.to_ulong(); ++bits) {
            TGroupMask groups(bits);
            operations.push_back({
                .Groups = groups,
                .Values = Format(
                    "%v%v%v",
                    (groups & PrimaryGroup).any() ? "c=new;" : "",
                    (groups & GroupA).any() ? "a=2;" : "",
                    (groups & GroupB).any() ? "b=6.28;" : ""),
                .LockType = lockType,
                .Kind = IsWritingLock(lockType) ? ELockMutation::Write : ELockMutation::Lock,
            });
        }
    }

    operations.push_back({PrimaryGroup, "", ELockType::Exclusive, ELockMutation::Delete});
    int cases = 0;
    for (const auto& owner : operations) {
        for (const auto& requester : operations) {
            auto groupPair = std::pair(owner.Groups, requester.Groups);
            if (owner.Kind != ELockMutation::Delete && requester.Kind != ELockMutation::Delete &&
                std::ranges::find(MutationGroupPairs_, groupPair) == MutationGroupPairs_.end())
            {
                continue;
            }

            for (const auto& timing : LockTimings) {
                RunMutationCase(++cases, owner, requester, timing);
                EXPECT_EQ(0, Store_->GetLockCount());

                if (HasFailure()) {
                    return;
                }
            }
        }
    }

    EXPECT_EQ(2504, cases);
}

////////////////////////////////////////////////////////////////////////////////

class TLockingMatrixSortedDynamicStoreTest
    : public TSortedLockingTest
    , public ::testing::WithParamInterface<std::tuple<TSortedLockingTest::TLockHistoryCase, TStringBuf, TStringBuf>>
{
protected:
    struct TReadCase
    {
        const TLockHistoryCase& History;
        const TLockMutationCase& Mutation;
        const TLockProjectionCase& Projection;
        TStringBuf Reader;
        TStringBuf Versions;
        TStringBuf Clock;
        TWriterMask CommitMask = 0;
    };

    struct TReadTimestamps
    {
        TTimestamp BeforePrepare;
        TTimestamp Prepare;
        TTimestamp AfterCommit;
        TTimestamp Read;
    };

    struct TReadResult
    {
        TUnversionedOwningRow Row;
        TWriterMask WaitedMask = 0;
    };

    // AtCommit is the first writer's commit; AfterCommit follows all writers' commits.
    static constexpr TStringBuf Clocks[] = {
        "BeforePrepare",
        "AtPrepare",
        "BeforeCommit",
        "AtCommit",
        "AfterCommit",
        "SyncLatest",
        "AsyncLatest",
    };

    static std::string DescribeReadCase(const TReadCase& testCase)
    {
        auto description = Format(
            "History=%v Mutation=%v Projection=%v Reader=%v Versions=%v Clock=%v",
            testCase.History.Name,
            testCase.Mutation.Name,
            testCase.Projection.Name,
            testCase.Reader,
            testCase.Versions,
            testCase.Clock);
        for (int index = 0; index < std::ssize(testCase.Mutation.Writers); ++index) {
            const auto& writer = testCase.Mutation.Writers[index];
            description += Format(
                "\nT%v: %v groups=%v %v",
                index + 1,
                writer.LockType,
                DescribeGroups(writer.Groups),
                testCase.CommitMask & (1U << index) ? "Commit" : "Abort");
        }

        return description;
    }

    // The model uses scenario data and assigned timestamps, never store descriptors.
    TWriterMask ComputeExpectedWaitMask(
        const TReadCase& testCase,
        const std::vector<TLockTransaction>& writers,
        TTimestamp prepare,
        TTimestamp timestamp) const
    {
        if (timestamp <= prepare || timestamp == AsyncLastCommittedTimestamp) {
            return 0;
        }

        TWriterMask waitMask = 0;
        bool rowExists = testCase.History.Exists;
        auto checkedGroups = testCase.Versions == "AllVersions"
            ? AllGroups
            : ComputeProjectionGroups(testCase.Projection.Filter);

        // First wait for writers touching the projection (including primary).
        for (int index = 0; index < std::ssize(writers); ++index) {
            const auto& writer = writers[index];
            if (!IsWritingLock(writer.Operation.LockType) || !(writer.Operation.Groups & checkedGroups).any()) {
                continue;
            }

            waitMask |= 1U << index;
            // A visible commit establishes existence, even for null values; a pure lock does not.
            rowExists |= writer.Operation.Kind == ELockMutation::Write &&
                (testCase.CommitMask & (1U << index)) && writer.Commit <= timestamp;
        }

        // If existence is still unknown, an unselected writer may insert/resurrect
        // the row. The reader then checks every group, so include all writing locks.
        if (!rowExists) {
            for (int index = 0; index < std::ssize(writers); ++index) {
                if (IsWritingLock(writers[index].Operation.LockType)) {
                    waitMask |= 1U << index;
                }
            }
        }

        return waitMask;
    }

    TUnversionedOwningRow ComputeExpectedRow(
        const TReadCase& testCase,
        TUnversionedRow initial,
        const std::vector<TLockTransaction>& writers,
        TWriterMask commitMask,
        TTimestamp timestamp) const
    {
        bool exists = testCase.History.Exists;
        std::array<TUnversionedValue, ColumnCount> values;
        for (int index = 0; index < ColumnCount; ++index) {
            values[index] = exists ? initial[index] : MakeUnversionedNullValue(index);
        }

        values[KeyColumnId] = initial[KeyColumnId];
        for (int index = 0; index < std::ssize(writers); ++index) {
            const auto& writer = writers[index];
            if (!(commitMask & (1U << index)) || writer.Commit > timestamp ||
                writer.Operation.Kind == ELockMutation::Lock)
            {
                continue;
            }

            if (writer.Operation.Kind == ELockMutation::Delete) {
                exists = false;
                continue;
            }

            exists = true;
            for (const auto& value : writer.Data) {
                values[value.Id] = value;
            }
        }

        if (!exists) {
            return {};
        }

        TUnversionedOwningRowBuilder builder;
        for (int index = 0; index < ColumnCount; ++index) {
            if (testCase.Projection.Filter.ContainsIndex(index)) {
                builder.AddValue(values[index]);
            }
        }

        return builder.FinishRow();
    }

    TUnversionedOwningRow ReadProjection(
        const TReadCase& testCase,
        const TTabletSnapshotPtr& snapshot,
        const TLegacyOwningKey& key,
        TTimestamp timestamp)
    {
        const auto& filter = testCase.Projection.Filter;
        auto createReader = [&] (const auto& keysOrRanges) {
            return Store_->CreateReader(
                snapshot,
                keysOrRanges,
                timestamp,
                testCase.Versions == "AllVersions",
                filter,
                ChunkReadOptions_,
                /*workloadCategory*/ std::nullopt);
        };

        auto reader = testCase.Reader == "Range"
            ? createReader(MakeSingletonRowRange(key, BuildKey(ToString(key[KeyColumnId].Data.Int64 + 1))))
            : createReader(MakeSharedRange(std::vector<TLegacyKey>{key.Get()}, key));
        WaitFor(reader->Open())
            .ThrowOnError();

        auto batch = reader->Read(TRowBatchReadOptions{.MaxRowsPerRead = 1});
        if (!batch || batch->GetRowCount() == 0) {
            return {};
        }

        // The merger also accepts a missing lookup row.
        NRowMerger::TSchemafulRowMerger merger(
            New<TRowBuffer>(),
            ColumnCount,
            /*keyColumnCount*/ 1,
            filter,
            snapshot->ColumnEvaluator);
        merger.AddPartialRow(batch->MaterializeRows().Front(), MaxTimestamp);

        return TUnversionedOwningRow(merger.BuildMergedRow());
    }

    TReadTimestamps AssignReadTimestamps(std::vector<TLockTransaction>* writers, TStringBuf clock)
    {
        TReadTimestamps timestamps{
            .BeforePrepare = GenerateTimestamp(),
            .Prepare = GenerateTimestamp(),
        };
        auto beforeCommit = GenerateTimestamp();
        for (auto& writer : *writers) {
            writer.Commit = GenerateTimestamp();
        }

        timestamps.AfterCommit = GenerateTimestamp();
        const THashMap<TStringBuf, TTimestamp> readTimestamps = {
            {"BeforePrepare", timestamps.BeforePrepare},
            {"AtPrepare", timestamps.Prepare},
            {"BeforeCommit", beforeCommit},
            {"AtCommit", writers->front().Commit},
            {"AfterCommit", timestamps.AfterCommit},
            {"SyncLatest", SyncLastCommittedTimestamp},
            {"AsyncLatest", AsyncLastCommittedTimestamp},
        };

        timestamps.Read = GetOrCrash(readTimestamps, clock);

        return timestamps;
    }

    void FinishWriters(std::vector<TLockTransaction>* writers, TWriterMask commitMask)
    {
        for (int index = 0; index < std::ssize(*writers); ++index) {
            auto& writer = (*writers)[index];
            if (!writer.Finished) {
                FinishLockTransaction(&writer, commitMask & (1U << index));
            }
        }
    }

    TWriterMask FinishBlockingWriter(std::vector<TLockTransaction>* writers, int lockIndex, TWriterMask commitMask)
    {
        for (int index = 0; index < std::ssize(*writers); ++index) {
            auto& writer = (*writers)[index];
            if (writer.Finished || !writer.Operation.Groups.test(lockIndex)) {
                continue;
            }

            TWriterMask writerBit = 1U << index;
            FinishLockTransaction(&writer, commitMask & writerBit);

            return writerBit;
        }

        THROW_ERROR_EXCEPTION("No pending writer owns reported blocking group %v", lockIndex);
    }

    // Resolve only writers reported as blockers, applying their commit/abort outcomes.
    // The caller checks both the resulting row and the identities of those writers.
    TReadResult ReadResolvingWriters(
        const TReadCase& testCase,
        const TTabletSnapshotPtr& snapshot,
        const TLegacyOwningKey& key,
        TTimestamp timestamp,
        std::vector<TLockTransaction>* writers)
    {
        TReadResult result;
        auto handlerGuard = Finally([&] {
            Store_->ResetRowBlockedHandler();
        });
        Store_->SetRowBlockedHandler(BIND([&] (
            TSortedDynamicRow row,
            TSortedDynamicStore::TConflictInfo conflict,
            TDuration /*timeout*/)
        {
            EXPECT_EQ(static_cast<TSortedDynamicRow>(writers->front().Row), row);
            result.WaitedMask |= FinishBlockingWriter(writers, conflict.LockIndex, testCase.CommitMask);

            return TSortedDynamicStore::TRowBlockedWaitingResult();
        }));

        result.Row = ReadProjection(testCase, snapshot, key, timestamp);

        return result;
    }

    void RunReadCase(const TTabletSnapshotPtr& snapshot, int keyValue, const TReadCase& testCase)
    {
        SCOPED_TRACE(DescribeReadCase(testCase));

        // 1. Seed a fresh key, then acquire all writers' locks without preparing them.
        const auto& history = testCase.History;
        auto key = BuildKey(ToString(keyValue));
        auto initial = BuildRow(Format("key=%v;%v", keyValue, history.Values ? history.Values : ""));
        if (history.Values) {
            WriteRow(initial);
        }

        if (history.Delete) {
            DeleteRow(key);
        }

        std::vector<TLockTransaction> writers;
        auto transactionsGuard = Finally([&] {
            for (auto& writer : writers) {
                AbortIfUnfinished(&writer);
            }
        });

        for (const auto& operation : testCase.Mutation.Writers) {
            writers.push_back(StartLockTransaction(keyValue, operation));
            ASSERT_TRUE(AcquireLocks(&writers.back()));
        }

        auto timestamps = AssignReadTimestamps(&writers, testCase.Clock);
        auto checkRow = [&] (TWriterMask commitMask, TTimestamp readTimestamp) {
            EXPECT_EQ(
                ComputeExpectedRow(testCase, initial, writers, commitMask, readTimestamp),
                ReadProjection(testCase, snapshot, key, readTimestamp));
        };

        // 2. Active writers are invisible and do not block; then prepare them all.
        checkRow(/*commitMask*/ 0, timestamps.BeforePrepare);
        for (auto& writer : writers) {
            PrepareLocks(&writer, timestamps.Prepare);
        }

        // 3. Predict the prepared read before executing it. AsyncLatest ignores
        // pending writes; other reads may see writers committed while waiting.
        auto visibleCommits = timestamps.Read == AsyncLastCommittedTimestamp ? 0 : testCase.CommitMask;
        auto expectedRow = ComputeExpectedRow(testCase, initial, writers, visibleCommits, timestamps.Read);
        auto expectedWaitMask = ComputeExpectedWaitMask(testCase, writers, timestamps.Prepare, timestamps.Read);
        auto result = ReadResolvingWriters(testCase, snapshot, key, timestamps.Read, &writers);
        EXPECT_EQ(expectedRow, result.Row);
        EXPECT_EQ(expectedWaitMask, result.WaitedMask);

        // 4. Finish the remaining writers and recheck the original and final snapshots.
        FinishWriters(&writers, testCase.CommitMask);
        checkRow(testCase.CommitMask, timestamps.Read);
        checkRow(testCase.CommitMask, timestamps.AfterCommit);
        EXPECT_EQ(0, Store_->GetLockCount());
    }
};

TEST_P(TLockingMatrixSortedDynamicStoreTest, Matrix)
{
    const auto& [history, reader, versions] = GetParam();
    auto snapshot = Tablet_->BuildSnapshot(/*slot*/ nullptr);
    int cases = 0;
    for (const auto& mutation : LockMutations_) {
        for (const auto& projection : LockProjections_) {
            // AllVersions checks all groups for any projection. Keep key-only, sparse
            // payload and universal reads; SingleVersion covers every column subset.
            if (versions == "AllVersions" && TStringBuf(projection.Name) != "Keys" &&
                TStringBuf(projection.Name) != "Peer" && !projection.Filter.IsUniversal())
            {
                continue;
            }

            for (auto clock : Clocks) {
                for (TWriterMask commitMask = 0; commitMask < (1U << mutation.Writers.size()); ++commitMask) {
                    RunReadCase(
                        snapshot,
                        ++cases,
                        TReadCase{
                            .History = history,
                            .Mutation = mutation,
                            .Projection = projection,
                            .Reader = reader,
                            .Versions = versions,
                            .Clock = clock,
                            .CommitMask = commitMask,
                        });

                    if (HasFailure()) {
                        return;
                    }
                }
            }
        }
    }

    EXPECT_EQ(versions == "AllVersions" ? 840 : 2800, cases);
}

INSTANTIATE_TEST_SUITE_P(
    Exhaustive,
    TLockingMatrixSortedDynamicStoreTest,
    ::testing::Combine(
        ::testing::ValuesIn(TSortedLockingTest::LockHistories),
        ::testing::Values(TStringBuf("Lookup"), TStringBuf("Range")),
        ::testing::Values(TStringBuf("SingleVersion"), TStringBuf("AllVersions"))),
    ([] (const auto& info) {
        const auto& [history, readerKind, versionMode] = info.param;
        return Format("%v_%v_%v", history.Name, readerKind, versionMode);
    }));

////////////////////////////////////////////////////////////////////////////////

class TAtomicSortedDynamicStoreTest
    : public TSingleLockSortedDynamicStoreTest
{
protected:
    TTableSchemaPtr GetSchema() const override
    {
        // NB: Key columns must go first.
        return New<TTableSchema>(std::vector{
            TColumnSchema(TColumnSchema("key", EValueType::Int64).SetSortOrder(ESortOrder::Ascending)),
            TColumnSchema(TColumnSchema("a", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("b", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("c", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("d", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("e", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("f", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("g", EValueType::Int64).SetLock("l1")),
            TColumnSchema(TColumnSchema("x", EValueType::Int64).SetLock("l2")),
            TColumnSchema(TColumnSchema("y", EValueType::Int64).SetLock("l3")),
            TColumnSchema(TColumnSchema("z", EValueType::Int64).SetLock("l4"))
        });
    }

    std::atomic<bool> Stopped = false;

    TSortedDynamicRow WriteVersioned(const TUnversionedOwningRow& row)
    {
        TWriteContext context;

        auto rowBuffer = New<TRowBuffer>();
        TVersionedRowBuilder rowBuilder(rowBuffer);

        int keyColumnCount = Tablet_->GetPhysicalSchema()->GetKeyColumnCount();

        for (int index = 0; index < keyColumnCount; ++index) {
            rowBuilder.AddKey(row[index]);
        }

        for (int index = keyColumnCount; index < row.GetCount(); ++index) {
            TVersionedValue value;
            static_cast<TUnversionedValue&>(value) = row[index];
            value.Timestamp = GenerateTimestamp();

            rowBuilder.AddValue(value);
        }

        return Store_->ModifyRow(rowBuilder.FinishRow(), &context);
    }

    void WriteRows()
    {
        auto tx1 = StartTransaction();
        auto tx2 = StartTransaction();
        auto tx3 = StartTransaction();
        auto tx4 = StartTransaction();

        i64 cellValue = std::rand() % 100;
        auto rowString = Format("key=1;a=%v;b=%v;c=%v;d=%v;e=%v;f=%v;g=%v;",
            cellValue, cellValue, cellValue, cellValue, cellValue, cellValue, cellValue);

        TLockMask lockMask;
        auto row1 = BuildRow(rowString, false);
        auto dynamicRow1 = WriteRow(tx1.get(), row1, false, lockMask);

        auto row2 = BuildRow("key=1;x=1;", false);
        auto dynamicRow2 = WriteRow(tx2.get(), row2, false, lockMask);

        auto row3 = BuildRow("key=1;y=2;", false);
        auto dynamicRow3 = WriteRow(tx3.get(), row3, false, lockMask);

        auto row4 = BuildRow("key=1;z=3;", false);
        auto dynamicRow4 = WriteRow(tx4.get(), row4, false, lockMask);

        PrepareTransaction(tx1.get());
        PrepareRow(tx1.get(), dynamicRow1);

        PrepareTransaction(tx2.get());
        PrepareRow(tx2.get(), dynamicRow2);

        PrepareTransaction(tx3.get());
        PrepareRow(tx3.get(), dynamicRow3);

        PrepareTransaction(tx4.get());
        PrepareRow(tx4.get(), dynamicRow4);

        // Generate timestamps.
        CommitTransaction(tx1.get());
        CommitTransaction(tx2.get());
        CommitTransaction(tx3.get());
        CommitTransaction(tx4.get());

        WriteRow(tx4.get(), dynamicRow4, row4);
        CommitRow(tx4.get(), dynamicRow4);
        WriteRow(tx3.get(), dynamicRow3, row3);
        CommitRow(tx3.get(), dynamicRow3);
        WriteRow(tx2.get(), dynamicRow2, row2);
        CommitRow(tx2.get(), dynamicRow2);
        WriteRow(tx1.get(), dynamicRow1, row1);
        CommitRow(tx1.get(), dynamicRow1);
    }

    TVersionedOwningRow VersionedLookupRow(const TLegacyOwningKey& key)
    {
        std::vector<TLegacyKey> lookupKeys(1, key.Get());
        auto lookupReader = Store_->CreateReader(
            Tablet_->BuildSnapshot(nullptr),
            MakeSharedRange(std::move(lookupKeys), key),
            AllCommittedTimestamp,
            true,
            TColumnFilter(),
            ChunkReadOptions_,
            /*workloadCategory*/ std::nullopt);

        WaitForFast(lookupReader->Open())
            .ThrowOnError();

        std::vector<TVersionedRow> rows;
        rows.reserve(1);

        TRowBatchReadOptions options{
            .MaxRowsPerRead = 1
        };

        auto batch = lookupReader->Read(options);
        EXPECT_TRUE(batch);
        EXPECT_EQ(1, batch->GetRowCount());
        return TVersionedOwningRow(batch->MaterializeRows().Front());
    }

    void ReadRowAndCheck()
    {
        auto key = BuildKey("1");
        auto owningRow = LookupRow(key, AsyncLastCommittedTimestamp);

        TUnversionedRow row = owningRow;
        for (int i = 2; i < 8; ++i) {
            EXPECT_EQ(row[1], row[i]);
        }
    }

    void SetUp() override
    {
        TSingleLockSortedDynamicStoreTest::SetUp();
    }
};

TEST_F(TAtomicSortedDynamicStoreTest, ReadAtomicity)
{
    // First lock group consists of self verified columns (values must be equal after each transaction write).
    // Other lock groups consists of one column.
    // Write any value in other lock groups.

    WriteRows();

    using TThis = typename std::remove_reference<decltype(*this)>::type;
    ::TThread thread1([] (void* opaque) -> void* {
        auto this_ = static_cast<TThis*>(opaque);
        while (!this_->Stopped.load()) {
            this_->WriteRows();
        }
        return nullptr;
    }, this);
    ::TThread thread2([] (void* opaque) -> void* {
        auto this_ = static_cast<TThis*>(opaque);
        while (!this_->Stopped.load()) {
            this_->ReadRowAndCheck();
        }
        return nullptr;
    }, this);

    thread1.Start();
    thread2.Start();

    Sleep(TDuration::Seconds(2));

    Stopped.store(true);

    thread1.Join();
    thread2.Join();
}

TEST_F(TAtomicSortedDynamicStoreTest, ReadAllCommittedWriteVersioned)
{
    auto row = BuildRow("key=1;a=1;b=2;c=3;d=4;e=5;f=6;g=7;", false);
    WriteVersioned(row);

    auto key = BuildKey("1");
    auto result = VersionedLookupRow(key);

    EXPECT_TRUE(result);

    EXPECT_EQ(result.Keys()[0], row[0]);
    for (int i = 1; i < 8; ++i) {
        EXPECT_EQ(static_cast<const TUnversionedValue&>(result.Values()[i - 1]), row[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TNonAtomicSortedDynamicStoreTest
    : public TSingleLockSortedDynamicStoreTest
{
protected:
    EAtomicity GetAtomicity() const override
    {
        return EAtomicity::None;
    }
};

TEST_F(TNonAtomicSortedDynamicStoreTest, Write1)
{
    auto key = BuildKey("1");

    auto row = WriteRowNonAtomic(BuildRow("key=1;a=1", false), 100_ts);

    EXPECT_EQ(100_ts, GetLastCommitTimestamp(row));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100_ts), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, AsyncLastCommittedTimestamp), "key=1;a=1"));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, Write2)
{
    auto key = BuildKey("1");
    auto rowStr1 = "key=1;a=1";
    auto rowStr2 = "key=1;b=3.14";

    auto row1 = WriteRowNonAtomic(BuildRow(rowStr1, false), 100_ts);
    EXPECT_EQ(100_ts, GetLastCommitTimestamp(row1));

    auto row2 = WriteRowNonAtomic(BuildRow(rowStr2, false), 200_ts);
    EXPECT_EQ(200_ts, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100_ts), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 200_ts), "key=1;a=1;b=3.14"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 300_ts), "key=1;a=1;b=3.14"));
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
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, timestamps[i]), std::string("key=1;a=" + ToString(i))));
    }
}

TEST_F(TNonAtomicSortedDynamicStoreTest, Delete1)
{
    auto key = BuildKey("1");
    auto row = DeleteRowNonAtomic(key, 100_ts);

    EXPECT_EQ(100_ts, GetLastCommitTimestamp(row));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, Delete2)
{
    auto key = BuildKey("1");

    auto row1 = DeleteRowNonAtomic(key, 100_ts);
    EXPECT_EQ(100_ts, GetLastCommitTimestamp(row1));

    auto row2 = DeleteRowNonAtomic(key, 200_ts);
    EXPECT_EQ(200_ts, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, WriteDelete1)
{
    auto key = BuildKey("1");

    auto row1 = WriteRowNonAtomic(BuildRow("key=1;a=1", false), 100_ts);
    EXPECT_EQ(100_ts, GetLastCommitTimestamp(row1));

    auto row2 = DeleteRowNonAtomic(key, 200_ts);
    EXPECT_EQ(200_ts, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100_ts), "key=1;a=1"));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 200_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, SyncLastCommittedTimestamp), nullptr));
}

TEST_F(TNonAtomicSortedDynamicStoreTest, WriteDelete2)
{
    auto key = BuildKey("1");

    auto row1 = DeleteRowNonAtomic(key, 100_ts);
    EXPECT_EQ(100_ts, GetLastCommitTimestamp(row1));

    auto row2 = WriteRowNonAtomic(BuildRow("key=1;a=1", false), 200_ts);
    EXPECT_EQ(200_ts, GetLastCommitTimestamp(row2));

    EXPECT_EQ(row1, row2);
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 99_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 100_ts), nullptr));
    EXPECT_TRUE(AreRowsEqual(LookupRow(key, 200_ts), "key=1;a=1"));
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
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, PrecedingTimestamp(writeTimestamps[i])), nullptr));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, writeTimestamps[i]), Format("key=%v;a=%v", i, i)));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, PrecedingTimestamp(deleteTimestamps[i])), Format("key=%v;a=%v", i, i)));
        EXPECT_TRUE(AreRowsEqual(LookupRow(key, deleteTimestamps[i]), nullptr));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
