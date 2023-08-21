#include <yt/yt/ytlib/table_client/key_set.h>
#include <yt/yt/ytlib/table_client/partitioner.h>

#include <yt/yt/client/table_client/comparator.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NTableClient {
namespace {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TUnversionedOwningRow MakeRow(std::vector<int> values, bool addMax = false)
{
    TUnversionedOwningRowBuilder builder;
    for (int value : values) {
        builder.AddValue(MakeUnversionedInt64Value(value));
    }
    if (addMax) {
        builder.AddValue(MakeUnversionedValueHeader(EValueType::Max));
    }

    return builder.FinishRow();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TPartitionerTest, Ordered)
{
    auto keySetWriter = New<TKeySetWriter>();
    keySetWriter->WriteKey(TUnversionedRow(MakeRow({1})));
    keySetWriter->WriteKey(TUnversionedRow(MakeRow({6}, /*addMax*/ true)));
    keySetWriter->WriteKey(TUnversionedRow(MakeRow({8})));
    keySetWriter->WriteKey(TUnversionedRow(MakeRow({8})));
    auto wirePivots = keySetWriter->Finish();

    TComparator comparator(std::vector<ESortOrder>({ESortOrder::Ascending}));
    auto partitioner = CreateOrderedPartitioner(wirePivots, comparator);

    EXPECT_EQ(5, partitioner->GetPartitionCount());
    EXPECT_EQ(0, partitioner->GetPartitionIndex(MakeRow({0})));
    EXPECT_EQ(1, partitioner->GetPartitionIndex(MakeRow({1})));
    EXPECT_EQ(1, partitioner->GetPartitionIndex(MakeRow({5})));
    EXPECT_EQ(1, partitioner->GetPartitionIndex(MakeRow({6})));
    EXPECT_EQ(1, partitioner->GetPartitionIndex(MakeRow({6, 42})));
    EXPECT_EQ(2, partitioner->GetPartitionIndex(MakeRow({7})));
    EXPECT_EQ(4, partitioner->GetPartitionIndex(MakeRow({42})));
}

TEST(TPartitionerTest, Hash)
{
    auto partitioner0 = CreateHashPartitioner(/*partitionCount*/ 10, /*keyColumnCount*/ 1, /*salt*/ 0);
    auto partitioner42 = CreateHashPartitioner(/*partitionCount*/ 7, /*keyColumnCount*/ 1, /*salt*/ 42);

    EXPECT_EQ(10, partitioner0->GetPartitionCount());
    EXPECT_EQ(7, partitioner42->GetPartitionCount());

    EXPECT_EQ(1, partitioner0->GetPartitionIndex(MakeRow({0})));
    EXPECT_EQ(1, partitioner0->GetPartitionIndex(MakeRow({0, 7})));
    EXPECT_EQ(9, partitioner0->GetPartitionIndex(MakeRow({35})));
    EXPECT_EQ(6, partitioner42->GetPartitionIndex(MakeRow({0})));
    EXPECT_EQ(5, partitioner42->GetPartitionIndex(MakeRow({37})));
    EXPECT_EQ(1, partitioner42->GetPartitionIndex(MakeRow({39})));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
