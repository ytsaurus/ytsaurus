#pragma once

#include "dynamic_store_ut_helpers.h"

namespace NYT::NTabletNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSortedStoreTestBase
    : public TDynamicStoreTestBase
{
public:
    void SetupTablet() override
    {
        Tablet_->CreateInitialPartition();
        Tablet_->StartEpoch(nullptr);
    }

    TTableSchemaPtr GetSchema() const override
    {
        // NB: Key columns must go first.
        return New<TTableSchema>(std::vector{
            TColumnSchema("key", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("b", EValueType::Double),
            TColumnSchema("c", EValueType::String)
        });
    }

    TUnversionedOwningRow LookupRow(
        ISortedStorePtr store,
        const TLegacyOwningKey& key,
        TTimestamp timestamp)
    {
        std::vector<TLegacyKey> lookupKeys(1, key.Get());
        auto lookupReader = store->CreateReader(
            Tablet_->BuildSnapshot(nullptr),
            MakeSharedRange(std::move(lookupKeys), key),
            timestamp,
            timestamp == AllCommittedTimestamp,
            TColumnFilter(),
            ChunkReadOptions_,
            /*workloadCategory*/ std::nullopt);

        lookupReader->Open()
            .Get()
            .ThrowOnError();

        std::vector<TVersionedRow> rows;
        rows.reserve(1);

        TRowBatchReadOptions options{
            .MaxRowsPerRead = 1
        };

        auto batch = lookupReader->Read(options);
        EXPECT_TRUE(batch);
        EXPECT_EQ(1, batch->GetRowCount());
        auto row = batch->MaterializeRows().Front();
        if (!row) {
            return TUnversionedOwningRow();
        }

        EXPECT_LE(row.GetWriteTimestampCount(), 1);
        EXPECT_LE(row.GetDeleteTimestampCount(), 1);
        if (row.GetWriteTimestampCount() == 0) {
            return TUnversionedOwningRow();
        }

        TUnversionedOwningRowBuilder builder;

        auto schema = Tablet_->GetPhysicalSchema();
        int keyCount = schema->GetKeyColumnCount();
        int schemaColumnCount = schema->GetColumnCount();

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

        return builder.FinishRow();
    }

    const TLockDescriptor& GetLock(
        TSortedDynamicRow row,
        int index = PrimaryLockIndex)
    {
        return row.BeginLocks(Tablet_->GetPhysicalSchema()->GetKeyColumnCount())[index];
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode

