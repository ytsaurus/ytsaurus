#pragma once

#include "stdafx.h"
#include "framework.h"
#include "versioned_table_client_ut.h"

#include <yt/core/misc/nullable.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/ytlib/new_table_client/public.h>
#include <yt/ytlib/new_table_client/schema.h>
#include <yt/ytlib/new_table_client/name_table.h>
#include <yt/ytlib/new_table_client/writer.h>
#include <yt/ytlib/new_table_client/schemaful_chunk_reader.h>
#include <yt/ytlib/new_table_client/schemaful_chunk_writer.h>
#include <yt/ytlib/new_table_client/versioned_row.h>
#include <yt/ytlib/new_table_client/unversioned_row.h>
#include <yt/ytlib/new_table_client/versioned_reader.h>

#include <yt/ytlib/chunk_client/config.h>
#include <yt/ytlib/chunk_client/memory_reader.h>
#include <yt/ytlib/chunk_client/memory_writer.h>

#include <yt/ytlib/tablet_client/public.h>
#include <yt/ytlib/tablet_client/config.h>

#include <yt/server/tablet_node/public.h>
#include <yt/server/tablet_node/config.h>
#include <yt/server/tablet_node/tablet.h>
#include <yt/server/tablet_node/tablet_manager.h>
#include <yt/server/tablet_node/transaction.h>
#include <yt/server/tablet_node/dynamic_memory_store.h>

namespace NYT {
namespace NTabletNode {
namespace {

using namespace NTabletClient;
using namespace NVersionedTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TMemoryStoreTestBase
    : public ::testing::Test
{
protected:
    TMemoryStoreTestBase()
        : CurrentTimestamp(MinTimestamp)
        , NameTable(New<TNameTable>())
    {
        TKeyColumns keyColumns;
        keyColumns.push_back("key");

        // NB: Key columns must go first.
        TTableSchema schema;
        schema.Columns().push_back(TColumnSchema("key", EValueType::Int64));
        schema.Columns().push_back(TColumnSchema("a", EValueType::Int64));
        schema.Columns().push_back(TColumnSchema("b", EValueType::Double));
        schema.Columns().push_back(TColumnSchema("c", EValueType::String));

        for (const auto& column : schema.Columns()) {
            NameTable->RegisterName(column.Name);
        }

        Tablet.reset(new TTablet(
            New<TTableMountConfig>(),
            New<TTabletWriterOptions>(),
            NullTabletId,
            nullptr,
            schema,
            keyColumns,
            MinKey(),
            MaxKey()));
    }

    TUnversionedOwningRow BuildRow(const Stroka& yson, bool treatMissingAsNull = true)
    {
        auto rowParts = ConvertTo<yhash_map<Stroka, INodePtr>>(
            TYsonString(yson, EYsonType::MapFragment));

        TUnversionedOwningRowBuilder rowBuilder;
        auto addValue = [&] (int id, INodePtr value) {
            switch (value->GetType()) {
                case ENodeType::Int64:
                    rowBuilder.AddValue(MakeUnversionedInt64Value(value->GetValue<i64>(), id));
                    break;
                case ENodeType::Double:
                    rowBuilder.AddValue(MakeUnversionedDoubleValue(value->GetValue<double>(), id));
                    break;
                case ENodeType::String:
                    rowBuilder.AddValue(MakeUnversionedStringValue(value->GetValue<Stroka>(), id));
                    break;
                default:
                    rowBuilder.AddValue(MakeUnversionedAnyValue(ConvertToYsonString(value).Data(), id));
                    break;
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
            } else if (treatMissingAsNull) {
                rowBuilder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
            }
        }

        // Variable values
        for (const auto& pair : rowParts) {
            int id = NameTable->GetIdOrRegisterName(pair.first);
            if (id >= Tablet->Schema().Columns().size()) {
                addValue(id, pair.second);
            }
        }

        return rowBuilder.GetRowAndReset();
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
        transaction->SetState(ETransactionState::TransientCommitPrepared);
    }

    void CommitTransaction(TTransaction* transaction)
    {
        ASSERT_EQ(transaction->GetState(), ETransactionState::TransientCommitPrepared);
        transaction->SetCommitTimestamp(GenerateTimestamp());
        transaction->SetState(ETransactionState::Committed);
    }

    void AbortTransaction(TTransaction* transaction)
    {
        transaction->SetState(ETransactionState::Aborted);
    }


    void CompareRows(const TUnversionedOwningRow& row, const TNullable<Stroka>& yson)
    {
        if (!row && !yson)
            return;

        ASSERT_TRUE(static_cast<bool>(row));
        ASSERT_TRUE(yson.HasValue());

        auto expectedRowParts = ConvertTo<yhash_map<Stroka, INodePtr>>(
            TYsonString(*yson, EYsonType::MapFragment));

        for (int index = 0; index < row.GetCount(); ++index) {
            const auto& value = row[index];
            const auto& name = NameTable->GetName(value.Id);
            auto it = expectedRowParts.find(name);
            switch (value.Type) {
                case EValueType::Int64:
                    ASSERT_TRUE(it != expectedRowParts.end());
                    ASSERT_EQ(
                        it->second->GetValue<i64>(),
                        value.Data.Int64);
                    break;

                case EValueType::Double:
                    ASSERT_TRUE(it != expectedRowParts.end());
                    ASSERT_EQ(
                        it->second->GetValue<double>(),
                        value.Data.Double);
                    break;

                case EValueType::String:
                    ASSERT_TRUE(it != expectedRowParts.end());
                    ASSERT_EQ(
                        it->second->GetValue<Stroka>(),
                        Stroka(value.Data.String, value.Length));
                    break;

                case EValueType::Null:
                    ASSERT_TRUE(it == expectedRowParts.end());
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }

    TUnversionedOwningRow LookupRow(IStorePtr store, const TOwningKey& key, TTimestamp timestamp)
    {
        auto keySuccessor = GetKeySuccessor(key.Get());
        auto reader = store->CreateReader(
            key,
            keySuccessor,
            timestamp,
            TColumnFilter());

        if (!reader) {
            return TUnversionedOwningRow();
        }

        THROW_ERROR_EXCEPTION_IF_FAILED(reader->Open().Get());

        std::vector<TVersionedRow> rows;
        rows.reserve(1);

        reader->Read(&rows);

        EXPECT_LE(rows.size(), 1);
        if (rows.empty()) {
            return TUnversionedOwningRow();
        }

        auto row = rows[0];

        EXPECT_EQ(row.GetTimestampCount(), 1);
        auto rowTimestamp = row.BeginTimestamps()[0];
        if (rowTimestamp & NTransactionClient::TombstoneTimestampMask) {
            return TUnversionedOwningRow();
        }

        TUnversionedOwningRowBuilder builder;
        
        int keyCount = static_cast<int>(Tablet->KeyColumns().size());
        int schemaColumnCount = static_cast<int>(Tablet->Schema().Columns().size());

        // Keys
        const auto* keys = row.BeginKeys();
        for (int id = 0; id < keyCount; ++id) {
            builder.AddValue(keys[id]);
        }

        // Fixed values
        int versionedIndex = 0;
        for (int id = keyCount; id < schemaColumnCount; ++id) {
            if (versionedIndex < row.GetValueCount() && row.BeginValues()[versionedIndex].Id == id) {
                builder.AddValue(row.BeginValues()[versionedIndex++]);
            } else {
                builder.AddValue(MakeUnversionedSentinelValue(EValueType::Null, id));
            }
        }

        return builder.GetRowAndReset();
    }


    TTimestamp CurrentTimestamp;
    TNameTablePtr NameTable;
    std::unique_ptr<TTablet> Tablet;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

