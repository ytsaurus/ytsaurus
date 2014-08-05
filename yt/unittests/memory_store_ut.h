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
        return NVersionedTableClient::BuildRow(yson, Tablet->KeyColumns(), Tablet->Schema(), treatMissingAsNull);
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

                case EValueType::Uint64:
                    ASSERT_TRUE(it != expectedRowParts.end());
                    ASSERT_EQ(
                        it->second->GetValue<ui64>(),
                        value.Data.Uint64);
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
        auto lookuper = store->CreateLookuper(timestamp, TColumnFilter());
        auto rowOrError = lookuper->Lookup(key.Get()).Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(rowOrError);

        auto row = rowOrError.Value();
        if (!row) {
            return TUnversionedOwningRow();
        }

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

