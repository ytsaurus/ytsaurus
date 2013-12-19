#include "stdafx.h"
#include "framework.h"

#include <yt/core/misc/nullable.h>

#include <yt/core/concurrency/fiber.h>

#include <yt/ytlib/new_table_client/public.h>
#include <yt/ytlib/new_table_client/schema.h>
#include <yt/ytlib/new_table_client/name_table.h>
#include <yt/ytlib/new_table_client/reader.h>
#include <yt/ytlib/new_table_client/writer.h>
#include <yt/ytlib/new_table_client/chunk_reader.h>
#include <yt/ytlib/new_table_client/chunk_writer.h>
#include <yt/ytlib/new_table_client/versioned_row.h>
#include <yt/ytlib/new_table_client/unversioned_row.h>

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
#include <yt/server/tablet_node/static_memory_store.h>
#include <yt/server/tablet_node/dynamic_memory_store.h>

#include "versioned_table_client_ut.h"

namespace NYT {
namespace {

using namespace NTabletClient;
using namespace NTabletNode;
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
            keyColumns,
            New<TTableMountConfig>()));
    }

    TUnversionedOwningRow BuildRow(const Stroka& yson, bool treatMissingAsNull = true)
    {
        auto rowParts = ConvertTo<yhash_map<Stroka, INodePtr>>(
            TYsonString(yson, EYsonType::MapFragment));

        TUnversionedOwningRowBuilder rowBuilder;
        auto addValue = [&] (int id, INodePtr value) {
            switch (value->GetType()) {
                case ENodeType::Integer:
                    rowBuilder.AddValue(MakeUnversionedIntegerValue(value->GetValue<i64>(), id));
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

        return rowBuilder.Finish();
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


    TTimestamp CurrentTimestamp;
    TNameTablePtr NameTable;
    std::unique_ptr<TTablet> Tablet;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

