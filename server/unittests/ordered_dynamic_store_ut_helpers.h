#pragma once

#include "dynamic_store_ut_helpers.h"

#include <yt/client/table_client/schemaful_reader.h>

namespace NYT {
namespace NTabletNode {
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

    virtual TTableSchema GetSchema() const override
    {
        return TTableSchema({
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("b", EValueType::Double),
            TColumnSchema("c", EValueType::String)
        });
    }

    TUnversionedOwningRow GetRow(IOrderedStorePtr store, i64 index)
    {
        // NB: Ordered reader accepts extended schema.
        TColumnFilter columnFilter;
        columnFilter.All = false;
        for (int id = 0; id < GetSchema().Columns().size(); ++id) {
            columnFilter.Indexes.push_back(id + 2);
        }
        auto reader = store->CreateReader(
            Tablet_->BuildSnapshot(nullptr),
            -1,
            store->GetStartingRowIndex() + index,
            store->GetStartingRowIndex() + index + 1,
            columnFilter,
            BlockReadOptions_);

        std::vector<TUnversionedRow> rows;
        rows.reserve(1);

        EXPECT_TRUE(reader->Read(&rows));
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
} // namespace NTabletNode
} // namespace NYT

