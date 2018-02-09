#pragma once

#include "dynamic_store_ut_helpers.h"

namespace NYT {
namespace NTabletNode {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSortedDynamicStoreTestBase
    : public TDynamicStoreTestBase
{
protected:
    virtual void SetupTablet() override
    {
        Tablet_->CreateInitialPartition();
        Tablet_->StartEpoch(nullptr);
    }

    virtual TTableSchema GetSchema() const override
    {
        // NB: Key columns must go first.
        return TTableSchema({
            TColumnSchema("key", EValueType::Int64)
                .SetSortOrder(ESortOrder::Ascending),
            TColumnSchema("a", EValueType::Int64),
            TColumnSchema("b", EValueType::Double),
            TColumnSchema("c", EValueType::String)
        });
    }

    TUnversionedOwningRow LookupRow(ISortedStorePtr store, const TOwningKey& key, TTimestamp timestamp)
    {
        std::vector<TKey> lookupKeys(1, key.Get());
        auto sharedLookupKeys = MakeSharedRange(std::move(lookupKeys), key);
        auto lookupReader = store->CreateReader(
            Tablet_->BuildSnapshot(nullptr),
            sharedLookupKeys,
            timestamp,
            timestamp == AllCommittedTimestamp,
            TColumnFilter(),
            TWorkloadDescriptor(),
            NChunkClient::TReadSessionId());

        lookupReader->Open()
            .Get()
            .ThrowOnError();

        std::vector<TVersionedRow> rows;
        rows.reserve(1);

        EXPECT_TRUE(lookupReader->Read(&rows));
        EXPECT_EQ(1, rows.size());
        auto row = rows.front();
        if (!row) {
            return TUnversionedOwningRow();
        }

        EXPECT_LE(row.GetWriteTimestampCount(), 1);
        EXPECT_LE(row.GetDeleteTimestampCount(), 1);
        if (row.GetWriteTimestampCount() == 0) {
            return TUnversionedOwningRow();
        }

        TUnversionedOwningRowBuilder builder;

        const auto schema = Tablet_->PhysicalSchema();
        int keyCount = schema.GetKeyColumnCount();
        int schemaColumnCount = schema.GetColumnCount();

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
        int index = TSortedDynamicRow::PrimaryLockIndex)
    {
        return row.BeginLocks(Tablet_->PhysicalSchema().GetKeyColumnCount())[index];
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NTabletNode
} // namespace NYT

