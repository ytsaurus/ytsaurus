#include "chunk_helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/node/tablet_node/sorted_chunk_store.h>

namespace NYT::NChunkServer {
namespace {

using namespace NTesting;

using namespace NObjectClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TLegacyReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TChunkViewTest
    : public TChunkGeneratorTestBase
{ };

////////////////////////////////////////////////////////////////////////////////

TEST_F(TChunkViewTest, TestRangesIntersection)
{
    const auto K5 = BuildKey("5");
    const auto K6 = BuildKey("6");
    const auto K7 = BuildKey("7");
    const auto K8 = BuildKey("8");
    const auto K9 = BuildKey("9");

    auto chunk = CreateChunk(0, 0, 0, 0, K5, K7);
    auto chunkView = CreateChunkView(chunk, K6, K8);

    TComparator comparator({ESortOrder::Ascending});

    EXPECT_EQ(K6, chunkView->Modifier().GetAdjustedLowerReadLimit(TLegacyReadLimit(K5)).GetLegacyKey());
    EXPECT_EQ(K8, chunkView->Modifier().GetAdjustedUpperReadLimit(TLegacyReadLimit(K9)).GetLegacyKey());

    auto completeRange = chunkView->GetCompleteReadRange(comparator);

    auto lowerBound = completeRange.LowerLimit().KeyBound();
    EXPECT_EQ(K6, lowerBound.Prefix);
    EXPECT_TRUE(lowerBound.IsInclusive);
    EXPECT_FALSE(lowerBound.IsUpper);

    auto upperBound = completeRange.UpperLimit().KeyBound();
    EXPECT_EQ(K7, upperBound.Prefix);
    EXPECT_TRUE(upperBound.IsInclusive);
    EXPECT_TRUE(upperBound.IsUpper);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
