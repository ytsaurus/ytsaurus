#include <yt/core/test_framework/framework.h>

#include "chunk_helpers.h"

#include <yt/server/master/chunk_server/chunk.h>
#include <yt/server/master/chunk_server/chunk_list.h>
#include <yt/server/master/chunk_server/helpers.h>

namespace NYT::NChunkServer {

namespace {

using namespace NTesting;
using namespace NChunkClient::NProto;
using namespace NObjectClient;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void CheckCumulativeStatistics(TChunkList* chunkList)
{
    TCumulativeStatisticsEntry current;

    int index = 0;
    for (auto* child : chunkList->Children()) {
        auto stats = GetChunkTreeStatistics(child);
        current = current + TCumulativeStatisticsEntry(stats);
        if (child->GetType() == EObjectType::ChunkList) {
            CheckCumulativeStatistics(child->AsChunkList());
        }
        if (chunkList->HasCumulativeStatistics()) {
            EXPECT_EQ(current, chunkList->CumulativeStatistics()[index]);
        }

        ++index;
    }

    EXPECT_EQ(current, TCumulativeStatisticsEntry(chunkList->Statistics()));
}

////////////////////////////////////////////////////////////////////////////////

class TChunkListCumulativeStatisticsTest
    : public TChunkGeneratorBase
{ };

TEST_F(TChunkListCumulativeStatisticsTest, Static)
{
    auto* root = CreateChunkList();
    AttachToChunkList(root, {CreateChunk(1, 2, 3, 4)});
    AttachToChunkList(root, {CreateChunk(5, 6, 7, 8)});

    auto* list = CreateChunkList();
    AttachToChunkList(list, {
        CreateChunk(5, 4, 3, 2),
        CreateChunk(7, 6, 5, 4),
        CreateChunk(3, 5, 7, 9),
    });

    AttachToChunkList(root, {
        list,
        CreateChunk(4, 2, 5, 3),
    });

    CheckCumulativeStatistics(root);
}

TEST_F(TChunkListCumulativeStatisticsTest, StaticWithEmpty)
{
    auto* root = CreateChunkList();
    AttachToChunkList(root, {
        CreateChunk(1, 2, 3, 4),
        CreateChunkList(),
    });
    AttachToChunkList(root, {CreateChunkList()});
    AttachToChunkList(root, {CreateChunk(4, 3, 2, 1)});
    AttachToChunkList(root, {
        CreateChunkList(),
        CreateChunk(3, 5, 7, 9),
    });

    CheckCumulativeStatistics(root);
}

TEST_F(TChunkListCumulativeStatisticsTest, OrderedTabletNoTrim)
{
    auto* root = CreateChunkList(EChunkListKind::OrderedDynamicTablet);

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    AttachToChunkList(root, {chunk1, chunk2});

    EXPECT_EQ(root->CumulativeStatistics()[0], (TCumulativeStatisticsEntry{1, 1, 1}));

    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    AttachToChunkList(root, {chunk3});
    EXPECT_EQ(root->CumulativeStatistics()[1], (TCumulativeStatisticsEntry{3, 2, 3}));
}

TEST_F(TChunkListCumulativeStatisticsTest, OrderedTabletTrim1)
{
    auto* root = CreateChunkList(EChunkListKind::OrderedDynamicTablet);

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    AttachToChunkList(root, {chunk1, chunk2});

    EXPECT_EQ(root->CumulativeStatistics()[0].RowCount, 1);
    EXPECT_EQ(root->CumulativeStatistics()[0].ChunkCount, 1);

    DetachFromChunkList(root, {chunk1});

    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    AttachToChunkList(root, {chunk3});
    EXPECT_EQ(root->CumulativeStatistics()[1].RowCount, 3);
    EXPECT_EQ(root->CumulativeStatistics()[1].ChunkCount, 2);
    // NB: Cannot compare DataSize here because its value in cumulative statistics
    // depends on the order in which chunks are trimmed and added.
}

TEST_F(TChunkListCumulativeStatisticsTest, OrderedTabletTrim2)
{
    auto* root = CreateChunkList(EChunkListKind::OrderedDynamicTablet);

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    AttachToChunkList(root, {chunk1, chunk2});

    EXPECT_EQ(root->CumulativeStatistics()[0].RowCount, 1);
    EXPECT_EQ(root->CumulativeStatistics()[0].ChunkCount, 1);

    DetachFromChunkList(root, {chunk1, chunk2});

    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    AttachToChunkList(root, {chunk3});
    EXPECT_EQ(root->CumulativeStatistics()[1].RowCount, 3);
    EXPECT_EQ(root->CumulativeStatistics()[1].ChunkCount, 2);
    // NB: Cannot compare DataSize here because its value in cumulative statistics
    // depends on the order in which chunks are trimmed and added.
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
