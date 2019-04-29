#include "chunk_helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/server/node/tablet_node/sorted_chunk_store.h>

namespace NYT::NChunkServer {
namespace {

using namespace NTesting;

using namespace NObjectClient;
using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TReadRange;
using NChunkClient::TReadLimit;

////////////////////////////////////////////////////////////////////////////////

class TChunkViewTest
    : public TChunkGeneratorBase
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

    EXPECT_EQ(K6, chunkView->GetAdjustedLowerReadLimit(TReadLimit(K5)).GetKey());
    EXPECT_EQ(K8, chunkView->GetAdjustedUpperReadLimit(TReadLimit(K9)).GetKey());
    auto completeRange = chunkView->GetCompleteReadRange();
    EXPECT_EQ(K6, completeRange.LowerLimit().GetKey());
    EXPECT_EQ(GetKeySuccessor(K7), completeRange.UpperLimit().GetKey());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChunkServer
