#pragma once

#include "dynamic_store_ut_helpers.h"

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>

namespace NYT::NTabletNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreTestBase
    : public TDynamicStoreTestBase
{
protected:
    virtual void SetupTablet() override
    {
        Tablet_->StartEpoch(nullptr);
    }

    virtual TTableSchemaPtr GetSchema() const override
    {
        return New<TTableSchema>(std::vector{
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("b", EValueType::Double),
            TColumnSchema("c", EValueType::String)
        });
    }

    TUnversionedOwningRow GetRow(IOrderedStorePtr store, i64 index)
    {
        // NB: Ordered reader accepts extended schema.
        TColumnFilter::TIndexes columnFilterIndexes;
        for (int id = 0; id < GetSchema()->Columns().size(); ++id) {
            columnFilterIndexes.push_back(id + 2);
        }
        auto columnFilter = TColumnFilter(std::move(columnFilterIndexes));
        auto reader = store->CreateReader(
            Tablet_->BuildSnapshot(nullptr),
            -1,
            store->GetStartingRowIndex() + index,
            store->GetStartingRowIndex() + index + 1,
            columnFilter,
            ChunkReadOptions_,
            /*workloadCategory*/ std::nullopt);

        NTableClient::TRowBatchReadOptions options{
            .MaxRowsPerRead = 1
        };
        auto batch = NTableClient::WaitForRowBatch(reader, options);
        EXPECT_TRUE(batch.operator bool());

        auto rows = batch->MaterializeRows();
        EXPECT_EQ(1, rows.size());
        auto row = rows[0];

        // NB: Ordered reader returns rows w.r.t. extended schema.
        TUnversionedOwningRowBuilder builder;
        for (int index = 0; index < row.GetCount(); ++index) {
            auto value = row[index];
            value.Id -= 2;
            builder.AddValue(value);
        }
        return builder.FinishRow();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

