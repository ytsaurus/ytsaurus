#include "stdafx.h"
#include "memory_store_ut.h"

#include <yt/ytlib/new_table_client/writer.h>
#include <yt/ytlib/new_table_client/chunk_writer.h>
#include <yt/ytlib/new_table_client/reader.h>
#include <yt/ytlib/new_table_client/chunk_reader.h>

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/memory_writer.h>
#include <yt/ytlib/chunk_client/memory_reader.h>

#include <yt/server/tablet_node/public.h>
#include <yt/server/tablet_node/config.h>
#include <yt/server/tablet_node/tablet_manager.h>
#include <yt/server/tablet_node/transaction.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

class TDynamicMemoryStoreTest
    : public TMemoryStoreTestBase
{
public:
    TDynamicMemoryStoreTest()
        : CurrentTimestamp(MinTimestamp)
    {
        auto config = New<TTabletManagerConfig>();
        Store = New<TDynamicMemoryStore>(config, Tablet.get());
    }


    TTimestamp GenerateTimestamp()
    {
        return CurrentTimestamp++;
    }


    std::unique_ptr<TTransaction> StartTransaction()
    {
        std::unique_ptr<TTransaction> transaction(new TTransaction(NullTransactionId));
        transaction->SetStartTimestamp(GenerateTimestamp());
        transaction->SetState(ETransactionState::Active);
        return transaction;
    }

    void PrepareTransaction(TTransaction* transaction)
    {
        ASSERT_EQ(transaction->GetState(), ETransactionState::Active);
        transaction->SetPrepareTimestamp(GenerateTimestamp());
        transaction->SetState(ETransactionState::TransientlyPrepared);
    }

    void CommitTransaction(TTransaction* transaction)
    {
        ASSERT_EQ(transaction->GetState(), ETransactionState::TransientlyPrepared);
        transaction->SetCommitTimestamp(GenerateTimestamp());
        transaction->SetState(ETransactionState::Committed);
    }

    void AbortTransaction(TTransaction* transaction)
    {
        transaction->SetState(ETransactionState::Aborted);
    }


    TDynamicRow WriteRow(
        TTransaction* transaction,
        TVersionedRow row,
        bool prewrite)
    {
        return Store->WriteRow(
            NameTable,
            transaction,
            row,
            prewrite);
    }

    TDynamicRow DeleteRow(
        TTransaction* transaction,
        NVersionedTableClient::TKey key,
        bool predelete)
    {
        return Store->DeleteRow(
            transaction,
            key,
            predelete);
    }

    TUnversionedOwningRow LookupRow(
        NVersionedTableClient::TKey key,
        TTimestamp timestamp)
    {
        auto scanner = Store->CreateScanner();
        auto scannerTimestamp = scanner->FindRow(key, timestamp);

        if (scannerTimestamp == NullTimestamp) {
            return TUnversionedOwningRow();
        }

        if (scannerTimestamp & TombstoneTimestampMask) {
            return TUnversionedOwningRow();
        }

        TUnversionedRowBuilder builder;
        
        int keyCount = static_cast<int>(Tablet->KeyColumns().size());
        int schemaColumnCount = static_cast<int>(Tablet->Schema().Columns().size());

        // Keys
        for (int index = 0; index < keyCount; ++index) {
            builder.AddValue(scanner->GetKey(index));
        }

        // Fixed values
        for (int index = 0; index < schemaColumnCount - keyCount; ++index) {
            const auto* value = scanner->GetFixedValue(index);
            builder.AddValue(
                value
                ? *static_cast<const TUnversionedValue*>(value)
                : MakeUnversionedSentinelValue(EValueType::Null, index + keyCount));
        }

        return builder.GetRow();
    }

    void CompareRows(TUnversionedRow row, const TNullable<Stroka>& yson)
    {
        if (!row && !yson)
            return;

        ASSERT_TRUE(static_cast<bool>(row));
        ASSERT_TRUE(yson.HasValue());

        auto expectedRowParts = ConvertTo<yhash_map<Stroka, INodePtr>>(TYsonString(*yson, EYsonType::MapFragment));

        for (int index = 0; index < row.GetValueCount(); ++index) {
            const auto& value = row[index];
            const auto& name = NameTable->GetName(value.Id);
            auto it = expectedRowParts.find(name);
            switch (value.Type) {
                case EValueType::Integer:
                    ASSERT_EQ(it->second->GetValue<i64>(), value.Data.Integer);
                    break;
                
                case EValueType::Double:
                    ASSERT_EQ(it->second->GetValue<double>(), value.Data.Double);
                    break;
                
                case EValueType::String:
                    ASSERT_EQ(it->second->GetValue<Stroka>(), Stroka(value.Data.String, value.Length));
                    break;

                case EValueType::Null:
                    ASSERT_TRUE(it == expectedRowParts.end());
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }


    TDynamicMemoryStorePtr Store;
    TTimestamp CurrentTimestamp;

};

///////////////////////////////////////////////////////////////////////////////

TEST_F(TDynamicMemoryStoreTest, Empty)
{
    auto key = BuildKey("1");
    CompareRows(LookupRow(key, 0), Null);
    CompareRows(LookupRow(key, LastCommittedTimestamp), Null);
}

TEST_F(TDynamicMemoryStoreTest, Write1)
{
    auto transaction = StartTransaction();

    auto key = BuildKey("1");

    Stroka rowString("key=1;a=1");
    
    CompareRows(LookupRow(key, LastCommittedTimestamp), Null);

    auto row = WriteRow(transaction.get(), BuildRow(rowString), true);
    ASSERT_EQ(row.GetTransaction(), transaction.get());
    ASSERT_TRUE(transaction->LockedRows().empty());

    Store->ConfirmRow(row);
    ASSERT_EQ(transaction->LockedRows().size(), 1);
    ASSERT_TRUE(transaction->LockedRows()[0].Row == row);

    CompareRows(LookupRow(key, LastCommittedTimestamp), Null);

    PrepareTransaction(transaction.get());
    Store->PrepareRow(row);

    CommitTransaction(transaction.get());
    Store->CommitRow(row);

    CompareRows(LookupRow(key, MinTimestamp), Null);
    CompareRows(LookupRow(key, LastCommittedTimestamp), rowString);
    CompareRows(LookupRow(key, MaxTimestamp), rowString);
    CompareRows(LookupRow(key, transaction->GetCommitTimestamp()), rowString);
    CompareRows(LookupRow(key, transaction->GetCommitTimestamp() - 1), Null);
}

TEST_F(TDynamicMemoryStoreTest, Write2)
{
    auto key = BuildKey("1");

    std::vector<TTimestamp> timestamps;

    for (int i = 0; i < 100; ++i) {
        auto transaction = StartTransaction();

        if (i == 0) {
            CompareRows(LookupRow(key, transaction->GetStartTimestamp()), Null);
        } else {
            CompareRows(LookupRow(key, transaction->GetStartTimestamp()), "key=1;a=" + ToString(i - 1));
        }

        auto row = WriteRow(transaction.get(), BuildRow("key=1;a=" + ToString(i)), false);

        PrepareTransaction(transaction.get());
        Store->PrepareRow(row);

        CommitTransaction(transaction.get());
        Store->CommitRow(row);

        timestamps.push_back(transaction->GetCommitTimestamp());
    }


    CompareRows(LookupRow(key, MinTimestamp), Null);
    CompareRows(LookupRow(key, MaxTimestamp), Stroka("key=1;a=99"));
    CompareRows(LookupRow(key, LastCommittedTimestamp), Stroka("key=1;a=99"));

    for (int i = 0; i < 100; ++i) {
        CompareRows(LookupRow(key, timestamps[i]), Stroka("key=1;a=" + ToString(i)));
    }
}

TEST_F(TDynamicMemoryStoreTest, Write3)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto row1 = WriteRow(transaction.get(), BuildRow("key=1;b=3.14"), false);
    auto row2 = WriteRow(transaction.get(), BuildRow("key=1;b=2.71"), false);
    ASSERT_TRUE(row1 == row2);

    PrepareTransaction(transaction.get());
    Store->PrepareRow(row1);

    CommitTransaction(transaction.get());
    Store->CommitRow(row1);

    CompareRows(LookupRow(key, LastCommittedTimestamp), Stroka("key=1;b=2.71"));
}

TEST_F(TDynamicMemoryStoreTest, Write4)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    WriteRow(transaction.get(), BuildRow("key=1;c=test"), true);
    ASSERT_ANY_THROW({
        DeleteRow(transaction.get(), key, true);
    });
}

TEST_F(TDynamicMemoryStoreTest, Delete1)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), key, false);

    CompareRows(LookupRow(key, MinTimestamp), Null);
    CompareRows(LookupRow(key, LastCommittedTimestamp), Null);
}

TEST_F(TDynamicMemoryStoreTest, Delete2)
{
    auto key = BuildKey("1");

    TTimestamp ts1;
    TTimestamp ts2;

    {
        auto transaction = StartTransaction();

        auto row = WriteRow(transaction.get(), BuildRow("key=1;c=value"), false);

        PrepareTransaction(transaction.get());
        Store->PrepareRow(row);

        CommitTransaction(transaction.get());
        Store->CommitRow(row);

        ts1 = transaction->GetCommitTimestamp();
    }

    {
        auto transaction = StartTransaction();

        auto row = DeleteRow(transaction.get(), key, true);

        PrepareTransaction(transaction.get());
        Store->PrepareRow(row);

        CommitTransaction(transaction.get());
        Store->CommitRow(row);

        ts2 = transaction->GetCommitTimestamp();
    }

    CompareRows(LookupRow(key, MinTimestamp), Null);
    CompareRows(LookupRow(key, ts1), Stroka("key=1;c=value"));
    CompareRows(LookupRow(key, ts2), Null);
}

TEST_F(TDynamicMemoryStoreTest, Conflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;c=test1"), true);
    ASSERT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;c=test2"), true);
    });
}

TEST_F(TDynamicMemoryStoreTest, Conflict2)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), key, true);
    ASSERT_ANY_THROW({
        WriteRow(transaction.get(), BuildRow("key=1"), true);
    });
}

TEST_F(TDynamicMemoryStoreTest, Conflict3)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto row = WriteRow(transaction1.get(), BuildRow("key=1;a=1"), true);

    PrepareTransaction(transaction1.get());
    Store->PrepareRow(row);

    CommitTransaction(transaction1.get());
    Store->CommitRow(row);

    ASSERT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;a=2"), true);
    });
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT
