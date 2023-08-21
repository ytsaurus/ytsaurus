#pragma once

#include "dynamic_store_ut_helpers.h"

#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/row_batch.h>

namespace NYT::NTabletNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

inline std::vector<TUnversionedOwningRow> ReadRowsImpl(
    const TOrderedDynamicStorePtr& store,
    int tabletIndex,
    i64 lowerRowIndex,
    i64 upperRowIndex,
    const TColumnFilter& columnFilter = TColumnFilter::MakeUniversal(),
    NChunkClient::TClientChunkReadOptions chunkReadOptions = {})
{
    auto reader = store->CreateReader(
        store->GetTablet()->BuildSnapshot(nullptr),
        tabletIndex,
        lowerRowIndex,
        upperRowIndex,
        AsyncLastCommittedTimestamp,
        columnFilter,
        chunkReadOptions,
        /*workloadCategory*/ std::nullopt);

    std::vector<TUnversionedOwningRow> allRows;
    TRowBatchReadOptions options{
        .MaxRowsPerRead = 100
    };
    while (auto batch = reader->Read(options)) {
        auto someRows = batch->MaterializeRows();
        YT_VERIFY(!someRows.empty());
        for (auto row : someRows) {
            allRows.push_back(TUnversionedOwningRow(row));
        }
    }
    return allRows;
}

////////////////////////////////////////////////////////////////////////////////

class TOrderedDynamicStoreTestBase
    : public TDynamicStoreTestBase
{
protected:
    void SetupTablet() override
    {
        Tablet_->StartEpoch(nullptr);
    }

    TTableSchemaPtr GetSchema() const override
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
        for (int id = 0; id < std::ssize(GetSchema()->Columns()); ++id) {
            columnFilterIndexes.push_back(id + 2);
        }
        auto columnFilter = TColumnFilter(std::move(columnFilterIndexes));
        auto reader = store->CreateReader(
            Tablet_->BuildSnapshot(nullptr),
            -1,
            store->GetStartingRowIndex() + index,
            store->GetStartingRowIndex() + index + 1,
            AsyncLastCommittedTimestamp,
            columnFilter,
            ChunkReadOptions_,
            /*workloadCategory*/ std::nullopt);

        NTableClient::TRowBatchReadOptions options{
            .MaxRowsPerRead = 1
        };
        auto batch = NTableClient::ReadRowBatch(reader, options);
        EXPECT_TRUE(batch.operator bool());

        auto rows = batch->MaterializeRows();
        EXPECT_EQ(1, std::ssize(rows));
        auto row = rows[0];

        // NB: Ordered reader returns rows w.r.t. extended schema.
        TUnversionedOwningRowBuilder builder;
        for (int index = 0; index < static_cast<int>(row.GetCount()); ++index) {
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

