#include <yt/yt/server/lib/chunk_pools/helpers.h>

#include <yt/yt/ytlib/chunk_pools/chunk_stripe.h>

#include <yt/yt/ytlib/chunk_client/input_chunk.h>
#include <yt/yt/ytlib/chunk_client/input_chunk_slice.h>
#include <yt/yt/ytlib/chunk_client/legacy_data_slice.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NControllerAgent {
namespace {

using namespace NChunkClient;
using namespace NChunkPools;
using namespace NCypressClient;
using namespace NObjectClient;

using ::testing::UnorderedElementsAre;

////////////////////////////////////////////////////////////////////////////////

class TChunkStripeMergerTest
    : public ::testing::Test
{
protected:
    std::mt19937_64 Gen_{42};

    TInputChunkPtr CreateChunk(
        i64 weight = 1_KB,
        i64 rowCount = 1000,
        i64 compressedSize = -1)
    {
        if (compressedSize == -1) {
            compressedSize = weight;
        }
        auto inputChunk = New<TInputChunk>();
        inputChunk->SetChunkId(MakeId(
            EObjectType::Chunk,
            TCellTag(0x42),
            std::uniform_int_distribution<ui64>()(Gen_),
            std::uniform_int_distribution<ui32>()(Gen_)));
        inputChunk->SetCompressedDataSize(compressedSize);
        inputChunk->SetTotalUncompressedDataSize(weight);
        inputChunk->SetTotalDataWeight(weight);
        inputChunk->SetTotalRowCount(rowCount);
        return inputChunk;
    }

    static TLegacyDataSlicePtr BuildDataSliceByChunk(const TInputChunkPtr& chunk)
    {
        auto chunkSlice = New<TInputChunkSlice>(chunk);
        auto dataSlice = CreateUnversionedInputDataSlice(chunkSlice);
        dataSlice->SetInputStreamIndex(chunk->GetTableIndex());
        dataSlice->TransformToNewKeyless();
        return dataSlice;
    }

    static TChunkStripePtr CreateStripe(const TInputChunkPtr& chunk)
    {
        auto dataSlice = BuildDataSliceByChunk(chunk);
        auto stripe = New<TChunkStripe>(dataSlice);
        return stripe;
    }

    static TChunkStripeListPtr CreateStripeList(
        std::vector<TInputChunkPtr> chunks,
        TPartitionTags partitionTags)
    {
        auto stripeList = New<TChunkStripeList>();
        i64 totalWeight = 0;
        i64 totalRowCount = 0;

        for (const auto& chunk : chunks) {
            stripeList->AddStripe(CreateStripe(chunk));
            totalWeight += chunk->GetDataWeight();
            totalRowCount += chunk->GetRowCount();
        }

        stripeList->SetFilteringPartitionTags(partitionTags, totalWeight, totalRowCount);
        return stripeList;
    }

    static std::vector<TChunkId> GetChunkIds(const TChunkStripeListPtr& stripeList)
    {
        std::vector<TChunkId> chunkIds;
        for (const auto& stripe : stripeList->Stripes()) {
            for (const auto& dataSlice : stripe->DataSlices()) {
                chunkIds.push_back(dataSlice->GetSingleUnversionedChunk()->GetChunkId());
            }
        }
        return chunkIds;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChunkStripeMergerTest, EmptyInput)
{
    auto result = MergeStripeLists({});

    EXPECT_EQ(result->Stripes().size(), 0u);
    EXPECT_FALSE(result->GetFilteringPartitionTags());

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre());
}

TEST_F(TChunkStripeMergerTest, SingleStripeList)
{
    auto chunk1 = CreateChunk(1_KB, 100);
    auto chunk2 = CreateChunk(2_KB, 200);

    auto stripeList = CreateStripeList({chunk1, chunk2}, /*partitionTags*/ {1, 2});

    auto result = MergeStripeLists({stripeList});

    EXPECT_EQ(result->Stripes().size(), 2u);
    EXPECT_TRUE(result->GetFilteringPartitionTags());

    auto tags = *result->GetFilteringPartitionTags();
    EXPECT_THAT(tags, UnorderedElementsAre(1, 2));

    auto stats = result->GetAggregateStatistics();
    EXPECT_EQ(stats.DataWeight, 3_KBs);
    EXPECT_EQ(stats.RowCount, 300);
    EXPECT_EQ(stats.ChunkCount, 2);
    EXPECT_EQ(stats.CompressedDataSize, 3_KBs);

    EXPECT_EQ(result->GetSliceCount(), 2);

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre(chunk1->GetChunkId(), chunk2->GetChunkId()));
}

TEST_F(TChunkStripeMergerTest, MultipleStripeLists)
{
    auto chunk1 = CreateChunk(1_KB, 100);
    auto chunk2 = CreateChunk(2_KB, 200);
    auto chunk3 = CreateChunk(3_KB, 300);

    auto stripeList1 = CreateStripeList({chunk1}, /*partitionTags*/ {1});
    auto stripeList2 = CreateStripeList({chunk2, chunk3}, /*partitionTags*/ {2, 3});

    auto result = MergeStripeLists({stripeList1, stripeList2});

    EXPECT_EQ(result->Stripes().size(), 3u);
    EXPECT_TRUE(result->GetFilteringPartitionTags());

    auto tags = *result->GetFilteringPartitionTags();
    EXPECT_THAT(tags, UnorderedElementsAre(1, 2, 3));

    auto stats = result->GetAggregateStatistics();
    EXPECT_EQ(stats.DataWeight, 6_KBs);
    EXPECT_EQ(stats.RowCount, 600);
    EXPECT_EQ(stats.ChunkCount, 3);
    EXPECT_EQ(stats.CompressedDataSize, 6_KBs);

    EXPECT_EQ(result->GetSliceCount(), 3);

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre(chunk1->GetChunkId(), chunk2->GetChunkId(), chunk3->GetChunkId()));
}

TEST_F(TChunkStripeMergerTest, DeduplicateChunks)
{
    auto chunk1 = CreateChunk(1_KB, 100);
    auto chunk2 = CreateChunk(2_KB, 200);
    auto chunk3 = CreateChunk(3_KB, 300);

    auto stripeList1 = New<TChunkStripeList>();
    stripeList1->AddStripe(CreateStripe(chunk1));
    stripeList1->AddStripe(CreateStripe(chunk2));
    stripeList1->SetFilteringPartitionTags(/*partitionTags*/ {1, 2}, /*dataWeight*/ 2_KBs, /*rowCount*/ 200);

    auto stripeList2 = New<TChunkStripeList>();
    stripeList2->AddStripe(CreateStripe(chunk2));
    stripeList2->AddStripe(CreateStripe(chunk3));
    stripeList2->SetFilteringPartitionTags(/*partitionTags*/ {2, 3}, /*dataWeight*/ 3_KBs, /*rowCount*/ 300);

    auto result = MergeStripeLists({stripeList1, stripeList2});

    EXPECT_EQ(result->Stripes().size(), 3u);
    EXPECT_TRUE(result->GetFilteringPartitionTags());

    auto tags = *result->GetFilteringPartitionTags();
    EXPECT_THAT(tags, UnorderedElementsAre(1, 2, 3));

    auto stats = result->GetAggregateStatistics();
    EXPECT_EQ(stats.DataWeight, 5_KBs);
    EXPECT_EQ(stats.RowCount, 500);
    EXPECT_EQ(stats.ChunkCount, 3);
    EXPECT_EQ(stats.CompressedDataSize, 6_KBs);

    EXPECT_EQ(result->GetSliceCount(), 3);

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre(chunk1->GetChunkId(), chunk2->GetChunkId(), chunk3->GetChunkId()));
}

TEST_F(TChunkStripeMergerTest, SkipEmptyStripeLists)
{
    auto chunk1 = CreateChunk(1_KB, 100);
    auto chunk2 = CreateChunk(2_KB, 200);

    auto stripeList1 = CreateStripeList({chunk1}, /*partitionTags*/ {1});

    auto emptyStripeList = New<TChunkStripeList>();
    emptyStripeList->SetFilteringPartitionTags({2}, /*dataWeight*/ 0, /*rowCount*/ 0);

    auto stripeList2 = CreateStripeList({chunk2}, /*partitionTags*/ {3});

    auto result = MergeStripeLists({stripeList1, emptyStripeList, stripeList2});

    EXPECT_EQ(result->Stripes().size(), 2u);
    EXPECT_TRUE(result->GetFilteringPartitionTags());

    auto tags = *result->GetFilteringPartitionTags();
    EXPECT_THAT(tags, UnorderedElementsAre(1, 3));

    auto stats = result->GetAggregateStatistics();
    EXPECT_EQ(stats.DataWeight, 3_KBs);
    EXPECT_EQ(stats.RowCount, 300);
    EXPECT_EQ(stats.ChunkCount, 2);
    EXPECT_EQ(stats.CompressedDataSize, 3_KBs);

    EXPECT_EQ(result->GetSliceCount(), 2);

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre(chunk1->GetChunkId(), chunk2->GetChunkId()));
}

TEST_F(TChunkStripeMergerTest, AllChunksDuplicated)
{
    auto chunk1 = CreateChunk(1_KB, 100);
    auto chunk2 = CreateChunk(2_KB, 200);

    auto stripeList1 = New<TChunkStripeList>();
    stripeList1->AddStripe(CreateStripe(chunk1));
    stripeList1->AddStripe(CreateStripe(chunk2));
    stripeList1->SetFilteringPartitionTags({1, 2}, /*dataWeight*/ 2_KBs, /*rowCount*/ 200);

    auto stripeList2 = New<TChunkStripeList>();
    stripeList2->AddStripe(CreateStripe(chunk1));
    stripeList2->AddStripe(CreateStripe(chunk2));
    stripeList2->SetFilteringPartitionTags({3, 4}, /*dataWeight*/ 1_KBs, /*rowCount*/ 100);

    auto result = MergeStripeLists({stripeList1, stripeList2});

    EXPECT_EQ(result->Stripes().size(), 2u);
    EXPECT_TRUE(result->GetFilteringPartitionTags());

    auto tags = *result->GetFilteringPartitionTags();
    EXPECT_THAT(tags, UnorderedElementsAre(1, 2, 3, 4));

    auto stats = result->GetAggregateStatistics();
    EXPECT_EQ(stats.DataWeight, 3_KBs);
    EXPECT_EQ(stats.RowCount, 300);
    EXPECT_EQ(stats.ChunkCount, 2);
    EXPECT_EQ(stats.CompressedDataSize, 3_KBs);

    EXPECT_EQ(result->GetSliceCount(), 2);

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre(chunk1->GetChunkId(), chunk2->GetChunkId()));
}

TEST_F(TChunkStripeMergerTest, NoDeduplicationWithoutPartitionTags)
{
    auto chunk1 = CreateChunk(1_KB, 100);
    auto chunk2 = CreateChunk(2_KB, 200);

    auto stripeList1 = New<TChunkStripeList>();
    stripeList1->AddStripe(CreateStripe(chunk1));
    stripeList1->AddStripe(CreateStripe(chunk2));

    auto stripeList2 = New<TChunkStripeList>();
    stripeList2->AddStripe(CreateStripe(chunk1));
    stripeList2->AddStripe(CreateStripe(chunk2));

    auto result = MergeStripeLists({stripeList1, stripeList2});

    EXPECT_EQ(result->Stripes().size(), 4u);
    EXPECT_FALSE(result->GetFilteringPartitionTags());

    auto stats = result->GetAggregateStatistics();
    EXPECT_EQ(stats.DataWeight, 6_KBs);
    EXPECT_EQ(stats.RowCount, 600);
    EXPECT_EQ(stats.ChunkCount, 4);
    EXPECT_EQ(stats.CompressedDataSize, 6_KBs);

    EXPECT_EQ(result->GetSliceCount(), 4);

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre(
        chunk1->GetChunkId(), chunk1->GetChunkId(),
        chunk2->GetChunkId(), chunk2->GetChunkId()));
}

TEST_F(TChunkStripeMergerTest, MultipleDataSlicesWithoutPartitionTags)
{
    auto chunk1 = CreateChunk(1_KB, 100);
    auto chunk2 = CreateChunk(2_KB, 200);
    auto chunk3 = CreateChunk(3_KB, 300);

    auto stripe1 = New<TChunkStripe>();
    stripe1->DataSlices().push_back(BuildDataSliceByChunk(chunk1));
    stripe1->DataSlices().push_back(BuildDataSliceByChunk(chunk2));

    auto stripe2 = New<TChunkStripe>();
    stripe2->DataSlices().push_back(BuildDataSliceByChunk(chunk3));

    auto stripeList = New<TChunkStripeList>();
    stripeList->AddStripe(stripe1);
    stripeList->AddStripe(stripe2);

    auto result = MergeStripeLists({stripeList});

    EXPECT_EQ(result->Stripes().size(), 2u);
    EXPECT_FALSE(result->GetFilteringPartitionTags());

    EXPECT_EQ(result->Stripes()[0]->DataSlices().size(), 2u);
    EXPECT_EQ(result->Stripes()[1]->DataSlices().size(), 1u);

    auto stats = result->GetAggregateStatistics();
    EXPECT_EQ(stats.DataWeight, 6_KBs);
    EXPECT_EQ(stats.RowCount, 600);
    EXPECT_EQ(stats.ChunkCount, 3);
    EXPECT_EQ(stats.CompressedDataSize, 6_KBs);

    EXPECT_EQ(result->GetSliceCount(), 3);

    auto chunkIds = GetChunkIds(result);
    EXPECT_THAT(chunkIds, UnorderedElementsAre(
        chunk1->GetChunkId(),
        chunk2->GetChunkId(),
        chunk3->GetChunkId()));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NControllerAgent
