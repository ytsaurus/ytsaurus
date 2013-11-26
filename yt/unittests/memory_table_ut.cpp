#include "stdafx.h"

#include <yt/core/misc/nullable.h>

#include <yt/core/yson/public.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/node.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/ytlib/new_table_client/public.h>
#include <yt/ytlib/new_table_client/config.h>
#include <yt/ytlib/new_table_client/schema.h>
#include <yt/ytlib/new_table_client/row.h>
#include <yt/ytlib/new_table_client/name_table.h>
#include <yt/ytlib/new_table_client/writer.h>
#include <yt/ytlib/new_table_client/chunk_writer.h>
#include <yt/ytlib/new_table_client/reader.h>
#include <yt/ytlib/new_table_client/chunk_reader.h>

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/memory_writer.h>
#include <yt/ytlib/chunk_client/memory_reader.h>

#include <yt/server/tablet_node/public.h>
#include <yt/server/tablet_node/config.h>
#include <yt/server/tablet_node/memory_table.h>
#include <yt/server/tablet_node/tablet_manager.h>
#include <yt/server/tablet_node/tablet.h>
#include <yt/server/tablet_node/transaction.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NTabletClient;
using namespace NVersionedTableClient;
using namespace NTransactionClient;
using namespace NChunkClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSingleMemoryTableTest
    : public ::testing::Test
{
public:
    TSingleMemoryTableTest()
        : CurrentTimestamp(MinTimestamp)
    {
        NameTable = New<TNameTable>();

        TKeyColumns keyColumns;
        keyColumns.push_back("key");

        TTableSchema schema;
        // NB: Key columns must go first.
        schema.Columns().push_back(TColumnSchema("key", EValueType::Integer));
        schema.Columns().push_back(TColumnSchema("a", EValueType::Integer));
        schema.Columns().push_back(TColumnSchema("b", EValueType::Double));
        schema.Columns().push_back(TColumnSchema("c", EValueType::String));

        for (const auto& column : schema.Columns()) {
            NameTable->RegisterName(column.Name);
        }

        Tablet.reset(new TTablet(
            NullTabletId,
            schema,
            keyColumns));

        auto config = New<TTabletManagerConfig>();
        Table = New<TMemoryTable>(config, Tablet.get());
    }


    TUnversionedOwningRow BuildKey(const Stroka& yson)
    {
        TUnversionedRowBuilder keyBuilder;
        std::vector<TYsonString> anyValues;
        auto keyParts = ConvertTo<std::vector<INodePtr>>(TYsonString(yson, EYsonType::ListFragment));
        for (auto keyPart : keyParts) {
            switch (keyPart->GetType()) {
                case ENodeType::Integer:
                    keyBuilder.AddValue(MakeIntegerValue<TUnversionedValue>(keyPart->GetValue<i64>()));
                    break;
                case ENodeType::Double:
                    keyBuilder.AddValue(MakeDoubleValue<TUnversionedValue>(keyPart->GetValue<double>()));
                    break;
                case ENodeType::String:
                    // NB: keyPart will hold the value.
                    keyBuilder.AddValue(MakeStringValue<TUnversionedValue>(keyPart->GetValue<Stroka>()));
                    break;
                default: {
                    // NB: Hold the serialized value explicitly.
                    auto anyValue = ConvertToYsonString(keyPart);
                    anyValues.push_back(anyValue);
                    keyBuilder.AddValue(MakeAnyValue<TUnversionedValue>(anyValue.Data()));
                    break;
                }
            }
        }
        return TUnversionedOwningRow(keyBuilder.GetRow());
    }

    TVersionedOwningRow BuildRow(const Stroka& yson)
    {
        auto rowParts = ConvertTo<yhash_map<Stroka, INodePtr>>(TYsonString(yson, EYsonType::MapFragment));

        TVersionedRowBuilder rowBuilder;
        std::vector<TYsonString> anyValues;
        auto addValue = [&] (int id, INodePtr value) {
            switch (value->GetType()) {
                case ENodeType::Integer:
                    rowBuilder.AddValue(MakeIntegerValue<TVersionedValue>(value->GetValue<i64>(), id));
                    break;
                case ENodeType::Double:
                    rowBuilder.AddValue(MakeDoubleValue<TVersionedValue>(value->GetValue<double>(), id));
                    break;
                case ENodeType::String:
                    // NB: keyPart will hold the value.
                    rowBuilder.AddValue(MakeStringValue<TVersionedValue>(value->GetValue<Stroka>(), id));
                    break;
                default: {
                    // NB: Hold the serialized value explicitly.
                    auto anyValue = ConvertToYsonString(value);
                    anyValues.push_back(anyValue);
                    rowBuilder.AddValue(MakeAnyValue<TVersionedValue>(anyValue.Data(), id));
                    break;
                }
            }
        };

        // Key
        for (int id = 0; id < static_cast<int>(Tablet->KeyColumns().size()); ++id) {
            auto it = rowParts.find(NameTable->GetName(id));
            YCHECK(it != rowParts.end());
            addValue(id, it->second);
        }

        // Fixed values
        for (int id = static_cast<int>(Tablet->KeyColumns().size()); id < static_cast<int>(Tablet->Schema().Columns().size()); ++id) {
            auto it = rowParts.find(NameTable->GetName(id));
            if (it != rowParts.end()) {
                addValue(id, it->second);
            } else {
                rowBuilder.AddValue(MakeSentinelValue<TVersionedValue>(EValueType::Null, id));
            }
        }

        // Variable values
        for (const auto& pair : rowParts) {
            int id = NameTable->GetIdOrRegisterName(pair.first);
            if (id >= Tablet->Schema().Columns().size()) {
                addValue(id, pair.second);
            }
        }

        return TVersionedOwningRow(rowBuilder.GetRow());
    }

    void CheckRow(TVersionedRow row, const TNullable<Stroka>& yson)
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


    TBucket WriteRow(
        TTransaction* transaction,
        TVersionedRow row,
        bool prewrite)
    {
        return Table->WriteRow(
            NameTable,
            transaction,
            row,
            prewrite);
    }

    TBucket DeleteRow(
        TTransaction* transaction,
        NVersionedTableClient::TKey key,
        bool predelete)
    {
        return Table->DeleteRow(
            transaction,
            key,
            predelete);
    }

    TVersionedOwningRow LookupRow(
        NVersionedTableClient::TKey key,
        TTimestamp timestamp,
        const TColumnFilter& columnFilter = TColumnFilter())
    {
        auto memoryWriter = New<TMemoryWriter>();

        auto chunkWriter = New<TChunkWriter>(
            New<TChunkWriterConfig>(),
            New<TEncodingWriterOptions>(),
            memoryWriter);

        Table->LookupRow(
            chunkWriter,
            key,
            timestamp,
            columnFilter);

        auto memoryReader = New<TMemoryReader>(
            std::move(memoryWriter->GetChunkMeta()),
            std::move(memoryWriter->GetBlocks()));

        auto chunkReader = CreateChunkReader(
            New<TChunkReaderConfig>(),
            memoryReader);

        auto nameTable = New<TNameTable>();

        {
            auto error = chunkReader->Open(
                nameTable,
                Tablet->Schema(),
                true).Get();
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        std::vector<TVersionedRow> rows;
        rows.reserve(1);
        if (chunkReader->Read(&rows)) {
            std::vector<TVersionedRow> moreRows;
            rows.reserve(1);
            EXPECT_FALSE(chunkReader->Read(&moreRows));
        }
        EXPECT_TRUE(rows.size() <= 1);

        return rows.empty()
            ? TVersionedOwningRow()
            : TVersionedOwningRow(rows[0]);
    }


    TNameTablePtr NameTable;
    std::unique_ptr<TTablet> Tablet;
    TMemoryTablePtr Table;
    TTimestamp CurrentTimestamp;

};

///////////////////////////////////////////////////////////////////////////////

TEST_F(TSingleMemoryTableTest, Empty)
{
    auto key = BuildKey("1");
    CheckRow(LookupRow(key, 0), Null);
    CheckRow(LookupRow(key, LastCommittedTimestamp), Null);
}

TEST_F(TSingleMemoryTableTest, Write1)
{
    auto transaction = StartTransaction();

    auto key = BuildKey("1");

    Stroka rowString("key=1;a=1");
    auto row = BuildRow(rowString);
    
    CheckRow(LookupRow(key, LastCommittedTimestamp), Null);

    auto bucket = WriteRow(transaction.get(), row, true);
    ASSERT_EQ(bucket.GetTransaction(), transaction.get());
    ASSERT_TRUE(transaction->LockedBuckets().empty());

    Table->ConfirmBucket(bucket);
    ASSERT_EQ(transaction->LockedBuckets().size(), 1);
    ASSERT_TRUE(transaction->LockedBuckets()[0].Bucket == bucket);

    CheckRow(LookupRow(key, LastCommittedTimestamp), Null);

    PrepareTransaction(transaction.get());
    Table->PrepareBucket(bucket);

    CommitTransaction(transaction.get());
    Table->CommitBucket(bucket);

    CheckRow(LookupRow(key, MinTimestamp), Null);
    CheckRow(LookupRow(key, LastCommittedTimestamp), rowString);
    CheckRow(LookupRow(key, MaxTimestamp), rowString);
    CheckRow(LookupRow(key, transaction->GetCommitTimestamp()), rowString);
    CheckRow(LookupRow(key, transaction->GetCommitTimestamp() - 1), Null);
}

TEST_F(TSingleMemoryTableTest, Write2)
{
    auto key = BuildKey("1");

    std::vector<TTimestamp> timestamps;

    for (int i = 0; i < 100; ++i) {
        auto transaction = StartTransaction();

        if (i == 0) {
            CheckRow(LookupRow(key, transaction->GetStartTimestamp()), Null);
        } else {
            CheckRow(LookupRow(key, transaction->GetStartTimestamp()), "key=1;a=" + ToString(i - 1));
        }

        auto bucket = WriteRow(transaction.get(), BuildRow("key=1;a=" + ToString(i)), false);

        PrepareTransaction(transaction.get());
        Table->PrepareBucket(bucket);

        CommitTransaction(transaction.get());
        Table->CommitBucket(bucket);

        timestamps.push_back(transaction->GetCommitTimestamp());
    }


    CheckRow(LookupRow(key, MinTimestamp), Null);
    CheckRow(LookupRow(key, MaxTimestamp), Stroka("key=1;a=99"));
    CheckRow(LookupRow(key, LastCommittedTimestamp), Stroka("key=1;a=99"));

    for (int i = 0; i < 100; ++i) {
        CheckRow(LookupRow(key, timestamps[i]), Stroka("key=1;a=" + ToString(i)));
    }
}

TEST_F(TSingleMemoryTableTest, Write3)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();

    auto bucket1 = WriteRow(transaction.get(), BuildRow("key=1;b=3.14"), false);
    auto bucket2 = WriteRow(transaction.get(), BuildRow("key=1;b=2.71"), false);
    ASSERT_TRUE(bucket1 == bucket2);

    PrepareTransaction(transaction.get());
    Table->PrepareBucket(bucket1);

    CommitTransaction(transaction.get());
    Table->CommitBucket(bucket1);

    CheckRow(LookupRow(key, LastCommittedTimestamp), Stroka("key=1;b=2.71"));
}

TEST_F(TSingleMemoryTableTest, Write4)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    WriteRow(transaction.get(), BuildRow("key=1;c=test"), true);
    ASSERT_ANY_THROW({
        DeleteRow(transaction.get(), key, true);
    });
}

TEST_F(TSingleMemoryTableTest, Delete1)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), key, false);

    CheckRow(LookupRow(key, MinTimestamp), Null);
    CheckRow(LookupRow(key, LastCommittedTimestamp), Null);
}

TEST_F(TSingleMemoryTableTest, Delete2)
{
    auto key = BuildKey("1");

    TTimestamp ts1;
    TTimestamp ts2;

    {
        auto transaction = StartTransaction();

        auto bucket = WriteRow(transaction.get(), BuildRow("key=1;c=value"), false);

        PrepareTransaction(transaction.get());
        Table->PrepareBucket(bucket);

        CommitTransaction(transaction.get());
        Table->CommitBucket(bucket);

        ts1 = transaction->GetCommitTimestamp();
    }

    {
        auto transaction = StartTransaction();

        auto bucket = DeleteRow(transaction.get(), key, true);

        PrepareTransaction(transaction.get());
        Table->PrepareBucket(bucket);

        CommitTransaction(transaction.get());
        Table->CommitBucket(bucket);

        ts2 = transaction->GetCommitTimestamp();
    }

    CheckRow(LookupRow(key, MinTimestamp), Null);
    CheckRow(LookupRow(key, ts1), Stroka("key=1;c=value"));
    CheckRow(LookupRow(key, ts2), Null);
}

TEST_F(TSingleMemoryTableTest, Conflict1)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();
    WriteRow(transaction1.get(), BuildRow("key=1;c=test1"), true);
    ASSERT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;c=test2"), true);
    });
}

TEST_F(TSingleMemoryTableTest, Conflict2)
{
    auto key = BuildKey("1");

    auto transaction = StartTransaction();
    DeleteRow(transaction.get(), key, true);
    ASSERT_ANY_THROW({
        WriteRow(transaction.get(), BuildRow("key=1"), true);
    });
}

TEST_F(TSingleMemoryTableTest, Conflict3)
{
    auto key = BuildKey("1");

    auto transaction1 = StartTransaction();
    auto transaction2 = StartTransaction();

    auto bucket = WriteRow(transaction1.get(), BuildRow("key=1;a=1"), true);

    PrepareTransaction(transaction1.get());
    Table->PrepareBucket(bucket);

    CommitTransaction(transaction1.get());
    Table->CommitBucket(bucket);

    ASSERT_ANY_THROW({
        WriteRow(transaction2.get(), BuildRow("key=1;a=2"), true);
    });
}

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT
