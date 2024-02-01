#include <yt/yt/server/node/tablet_node/replication_log.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/versioned_row.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTabletNode {
namespace {

using namespace NApi;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TEST(TReplicationLogTest, TestDeleteMessagesBuilding)
{
    auto rowBuffer = New<TRowBuffer>();
    auto versionedRowBuilder = TVersionedRowBuilder(rowBuffer);

    auto tableSchema = New<TTableSchema>(
        std::vector{
            TColumnSchema("key1", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("key2", EValueType::Int64, ESortOrder::Ascending),
            TColumnSchema("value1", EValueType::String)
        }
    );

    versionedRowBuilder.AddKey(MakeUnversionedInt64Value(1, 0));
    versionedRowBuilder.AddKey(MakeUnversionedInt64Value(2, 1));
    versionedRowBuilder.AddDeleteTimestamp(0x123);
    auto versionedRow = versionedRowBuilder.FinishRow();

    TUnversionedRowBuilder unversionedRowBuilder;
    auto actualLogRow = BuildLogRow(versionedRow, tableSchema, &unversionedRowBuilder);

    TUnversionedRowBuilder expectedRowBuilder;
    expectedRowBuilder.AddValue(MakeUnversionedUint64Value(0x123, 0));
    expectedRowBuilder.AddValue(MakeUnversionedInt64Value(static_cast<int>(ERowModificationType::Delete), 1));
    expectedRowBuilder.AddValue(MakeUnversionedInt64Value(1, 2));
    expectedRowBuilder.AddValue(MakeUnversionedInt64Value(2, 3));
    TUnversionedRow expectedLogRow = expectedRowBuilder.GetRow();

    ASSERT_EQ(actualLogRow, expectedLogRow);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTabletNode
