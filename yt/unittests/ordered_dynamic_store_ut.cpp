#include "ordered_dynamic_store_ut_helpers.h"

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NTransactionClient;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreTest
    : public TOrderedDynamicStoreTestBase
{
protected:
    virtual void SetUp() override
    {
        TOrderedDynamicStoreTestBase::SetUp();
        CreateDynamicStore();
    }


    void ConfirmRow(TTransaction* transaction, TOrderedDynamicRow row)
    {
        transaction->LockedOrderedRows().push_back(TOrderedDynamicRowRef(Store_.Get(), nullptr, row));
    }

    void PrepareRow(TTransaction* transaction, TOrderedDynamicRow row)
    {
        Store_->PrepareRow(transaction, row);
    }

    void CommitRow(TTransaction* transaction, TOrderedDynamicRow row)
    {
        Store_->CommitRow(transaction, row);
    }

    void AbortRow(TTransaction* transaction, TOrderedDynamicRow row)
    {
        Store_->AbortRow(transaction, row);
    }


    TOrderedDynamicRow WriteRow(
        TTransaction* transaction,
        const TUnversionedOwningRow& row,
        bool prelock)
    {
        auto dynamicRow = Store_->WriteRow(transaction, row);
        LockRow(transaction, prelock, dynamicRow);
        return dynamicRow;
    }

    TTimestamp WriteRow(const TUnversionedOwningRow& row)
    {
        auto transaction = StartTransaction();
        auto dynamicRow = WriteRow(transaction.get(), row, false);
        PrepareTransaction(transaction.get());
        PrepareRow(transaction.get(), dynamicRow);
        auto ts = CommitTransaction(transaction.get());
        CommitRow(transaction.get(), dynamicRow);
        return ts;
    }


    Stroka DumpStore()
    {
        TStringBuilder builder;
        builder.AppendFormat("RowCount=%v ValueCount=%v\n",
            Store_->GetRowCount(),
            Store_->GetValueCount());

        int schemaColumnCount = Tablet_->GetSchemaColumnCount();
        for (auto row : Store_->GetAllRows()) {
            builder.AppendChar('[');
            for (int i = 0; i < schemaColumnCount; ++i) {
                builder.AppendFormat(" %v", row[i]);
            }
            builder.AppendString(" ]");
            builder.AppendChar('\n');
        }
        return builder.Flush();
    }


    TOrderedDynamicStorePtr Store_;

private:
    virtual void CreateDynamicStore() override
    {
        auto config = New<TTabletManagerConfig>();
        Store_ = New<TOrderedDynamicStore>(
            config,
            TTabletId(),
            Tablet_.get());
    }

    virtual IDynamicStorePtr GetDynamicStore() override
    {
        return Store_;
    }


    void LockRow(TTransaction* transaction, bool prelock, TOrderedDynamicRow row)
    {
        auto rowRef = TOrderedDynamicRowRef(Store_.Get(), nullptr, row);
        TOrderedStoreManager::LockRow(transaction, prelock, rowRef);
    }
};

///////////////////////////////////////////////////////////////////////////////

TEST_F(TOrderedDynamicStoreTest, Empty)
{
    EXPECT_EQ(0, Store_->GetRowCount());
    EXPECT_EQ(0, Store_->GetValueCount());
}

TEST_F(TOrderedDynamicStoreTest, TransactionalWrite)
{
    EXPECT_EQ(0, Store_->GetRowCount());
    EXPECT_EQ(0, Store_->GetValueCount());

    auto transaction = StartTransaction();
    auto dynamicRow = WriteRow(transaction.get(), BuildRow("a=1"), false);
    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), dynamicRow);

    EXPECT_EQ(0, Store_->GetRowCount());
    EXPECT_EQ(0, Store_->GetValueCount());

    CommitTransaction(transaction.get());
    CommitRow(transaction.get(), dynamicRow);

    EXPECT_EQ(1, Store_->GetRowCount());
    EXPECT_EQ(3, Store_->GetValueCount());
}

TEST_F(TOrderedDynamicStoreTest, SerializeEmpty)
{
    auto check = [&] () {
        EXPECT_EQ(0, Store_->GetRowCount());
        EXPECT_EQ(0, Store_->GetValueCount());
    };

    check();

    ReserializeStore();

    check();
}

TEST_F(TOrderedDynamicStoreTest, SerializeNonempty1)
{
    WriteRow(BuildRow("a=1;b=3.14"));
    WriteRow(BuildRow("c=test"));

    auto check = [&] () {
        EXPECT_EQ(2, Store_->GetRowCount());
        EXPECT_EQ(6, Store_->GetValueCount());
    };

    check();

    ReserializeStore();

    check();
}

TEST_F(TOrderedDynamicStoreTest, SerializeNonempty2)
{
    WriteRow(BuildRow("a=1;b=3.14"));

    auto transaction = StartTransaction();
    auto dynamicRow = WriteRow(transaction.get(), BuildRow("c=test"), false);
    PrepareTransaction(transaction.get());
    PrepareRow(transaction.get(), dynamicRow);

    auto check = [&] () {
        EXPECT_EQ(1, Store_->GetRowCount());
        EXPECT_EQ(3, Store_->GetValueCount());
    };

    check();

    ReserializeStore();

    check();
}

///////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreWriteTest
    : public TOrderedDynamicStoreTest
    , public ::testing::WithParamInterface<int>
{ };

TEST_P(TOrderedDynamicStoreWriteTest, Write)
{
    EXPECT_EQ(0, Store_->GetRowCount());
    EXPECT_EQ(0, Store_->GetValueCount());

    int count = GetParam();
    for (int i = 0; i < count; ++i) {
        EXPECT_EQ(i, Store_->GetRowCount());
        EXPECT_EQ(i * 3, Store_->GetValueCount());
        WriteRow(BuildRow(Format("a=%v", i)));
    }

    auto rows = Store_->GetAllRows();
    EXPECT_EQ(count, rows.size());
    for (int i = 0; i < count; ++i) {
        EXPECT_TRUE(AreRowsEqual(rows[i], Format("a=%v", i)));
    }
}

INSTANTIATE_TEST_CASE_P(
    Write,
    TOrderedDynamicStoreWriteTest,
    ::testing::Values(
        1,
        10,
        1000,
        2000,
        10000));

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

