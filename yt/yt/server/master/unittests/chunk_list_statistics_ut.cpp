#include <yt/yt/core/test_framework/framework.h>

#include "chunk_helpers.h"

#include <yt/yt/server/master/chunk_server/chunk.h>
#include <yt/yt/server/master/chunk_server/chunk_list.h>
#include <yt/yt/server/master/chunk_server/helpers.h>

#include <random>

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
    for (auto child : chunkList->Children()) {
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

[[maybe_unused]]
std::ostream& operator << (std::ostream& os, const TCumulativeStatisticsEntry& entry)
{
    return os << Format("{RowCount=%v;ChunkCount=%v;DataSize=%v}",
        entry.RowCount,
        entry.ChunkCount,
        entry.DataSize).data();
}

////////////////////////////////////////////////////////////////////////////////

class TChunkListCumulativeStatisticsTest
    : public TChunkGeneratorTestBase
{ };

TEST_F(TChunkListCumulativeStatisticsTest, CannotAttachSealedAfterUnsealed1)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, false)});
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(true, false)}),
        TErrorException);
}

TEST_F(TChunkListCumulativeStatisticsTest, CannotAttachSealedAfterUnsealed2)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false), CreateJournalChunk(true, false)}),
        TErrorException);
}

TEST_F(TChunkListCumulativeStatisticsTest, CanAttachUnsealedAfterSealed)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(true, false)});
    AttachToChunkList(root, {CreateJournalChunk(false, false)});
}

TEST_F(TChunkListCumulativeStatisticsTest, CannotHaveMultipleNonoverlayedUnsealed1)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, false)});
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false)}),
        TErrorException);
}

TEST_F(TChunkListCumulativeStatisticsTest, CannotHaveMultipleNonoverlayedUnsealed2)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false), CreateJournalChunk(false, false)}),
        TErrorException);
}

TEST_F(TChunkListCumulativeStatisticsTest, CannotHaveNonoverlayedAfterOverlayed)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, true)});
    EXPECT_THROW(
        AttachToChunkList(root, {CreateJournalChunk(false, false)}),
        TErrorException);
}

TEST_F(TChunkListCumulativeStatisticsTest, CanHaveMultipleOverlayedUnsealed1)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, true)});
    AttachToChunkList(root, {CreateJournalChunk(false, true)});
}

TEST_F(TChunkListCumulativeStatisticsTest, CanHaveMultipleOverlayedUnsealed2)
{
    auto* root = CreateChunkList(EChunkListKind::JournalRoot);
    AttachToChunkList(root, {CreateJournalChunk(false, true), CreateJournalChunk(false, true)});
}

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

    EXPECT_EQ(root->CumulativeStatistics()[0], TCumulativeStatisticsEntry(1, 1, 1));

    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    AttachToChunkList(root, {chunk3});
    EXPECT_EQ(root->CumulativeStatistics()[1], TCumulativeStatisticsEntry(3, 2, 3));
}

TEST_F(TChunkListCumulativeStatisticsTest, OrderedTabletTrim)
{
    auto* root = CreateChunkList(EChunkListKind::OrderedDynamicTablet);

    auto* chunk1 = CreateChunk(1, 1, 1, 1);
    auto* chunk2 = CreateChunk(2, 2, 2, 2);
    AttachToChunkList(root, {chunk1, chunk2});

    EXPECT_EQ(root->CumulativeStatistics()[0].RowCount, 1);
    EXPECT_EQ(root->CumulativeStatistics()[0].ChunkCount, 1);

    DetachFromChunkList(root, {chunk1}, EChunkDetachPolicy::OrderedTabletPrefix);

    auto* chunk3 = CreateChunk(3, 3, 3, 3);
    AttachToChunkList(root, {chunk3});
    EXPECT_EQ(root->CumulativeStatistics()[1], TCumulativeStatisticsEntry(3, 1, 3));
    EXPECT_EQ(root->CumulativeStatistics()[2], TCumulativeStatisticsEntry(6, 2, 6));
}

TEST_F(TChunkListCumulativeStatisticsTest, OrderedTabletPhysicalTrim)
{
    auto* root = CreateChunkList(EChunkListKind::OrderedDynamicTablet);

    auto* previousChunk = CreateChunk(1, 1, 1, 1);
    auto* chunk = CreateChunk(1, 1, 1, 1);
    AttachToChunkList(root, {previousChunk});

    for (int i = 0; i < 17; ++i) {
        AttachToChunkList(root, {chunk});
        DetachFromChunkList(root, {previousChunk}, EChunkDetachPolicy::OrderedTabletPrefix);
        previousChunk = chunk;
        chunk = CreateChunk(1, 1, 1, 1);
    }

    // Children are erased from the chunk list in portions of at least 17 chunks.
    EXPECT_LT(root->Children().size(), 18u);
    int actualChunkCount = root->Children().size();
    EXPECT_EQ(root->CumulativeStatistics().Back(), TCumulativeStatisticsEntry(18, actualChunkCount, 18));
}

TEST_F(TChunkListCumulativeStatisticsTest, UnconfirmedChunk)
{
    auto* root = CreateChunkList(EChunkListKind::Static);
    auto* chunkList = CreateChunkList(EChunkListKind::Static);
    AttachToChunkList(root, {chunkList});

    int sum = 0;
    for (int i = 1; i <= 3; ++i) {
        auto* chunk = CreateUnconfirmedChunk();
        AttachToChunkList(chunkList, {chunk});

        const auto& stats = chunkList->CumulativeStatistics();
        EXPECT_EQ(static_cast<ssize_t>(stats.Size()), i);
        EXPECT_EQ(stats.Back().ChunkCount, i - 1);
        EXPECT_EQ(stats.Back().RowCount, sum);

        ConfirmChunk(chunk, i, i, i, i);
        ASSERT_TRUE(chunk->IsConfirmed());

        // The following lines mimic to those in TChunkManager::TImpl::OnChunkSealed.
        YT_VERIFY(chunk->GetParentCount() == 1);
        auto statisticsDelta = chunk->GetStatistics();
        AccumulateUniqueAncestorsStatistics(chunk, statisticsDelta);

        sum += i;

        EXPECT_EQ(stats.Back().ChunkCount, i);
        EXPECT_EQ(stats.Back().RowCount, sum);
    }

    EXPECT_EQ(chunkList->Statistics().ChunkCount, 3);
    EXPECT_EQ(root->Statistics().ChunkCount, 3);
    const auto& rootCumulativeStats = root->CumulativeStatistics();
    EXPECT_EQ(rootCumulativeStats.Size(), 1);
    EXPECT_EQ(rootCumulativeStats[0].ChunkCount, 3);
    EXPECT_EQ(rootCumulativeStats[0].RowCount, sum);
}

TEST_F(TChunkListCumulativeStatisticsTest, SortedDynamicRootChanging)
{
    // TRandomGenerator behaves badly in low bits. Hopefully, mt19937 generated sequence
    // is defined in the C++ standard.
    std::mt19937 gen(12345);

    auto* root = CreateChunkList(EChunkListKind::SortedDynamicRoot);
    std::vector<TChunkTreeRawPtr> tablets;
    for (int i = 0; i < 5; ++i) {
        auto* tablet = CreateChunkList(EChunkListKind::SortedDynamicTablet);
        tablets.push_back(tablet);
        std::vector<TChunkTreeRawPtr> chunks;
        for (int j = 0; j < 5; ++j) {
            int rowCount = gen() % 10 + 1;
            int dataWeight = gen() % 10 + 1;
            auto* chunk = CreateChunk(rowCount, dataWeight, dataWeight, 1);
            chunks.push_back(chunk);
        }
        AttachToChunkList(tablet, chunks);
    }

    AttachToChunkList(root, tablets);

    CheckCumulativeStatistics(root);

    for (int iter = 0; iter < 5; ++iter) {
        for (int i = 0; i < 5; ++i) {
            auto* tablet = tablets[i]->AsChunkList();
            // Always remove from the 3rd tablet and randomly insert/erase into others.
            if (i != 3 && gen() % 2) {
                int rowCount = gen() % 10 + 1;
                int dataWeight = gen() % 10 + 1;
                auto* chunk = CreateChunk(rowCount, dataWeight, dataWeight, 1);
                AttachToChunkList(tablet, {chunk});
            } else {
                auto& children = tablet->Children();
                auto randomChild = children[gen() % children.size()];
                DetachFromChunkList(tablet, {randomChild}, EChunkDetachPolicy::SortedTablet);
            }

            CheckCumulativeStatistics(root);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
