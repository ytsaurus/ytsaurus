#include <yt/yt/flow/library/cpp/pipeline_helpers/flow_execute/flow_execute.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowExecuteTest, CompressedFlowViewRoundTrip)
{
    auto keeper = New<TFlowViewKeeper>();
    keeper->Init(New<TFlowState>(), New<TFlowEphemeralState>(), New<TVersionedPipelineSpec>(), New<TVersionedDynamicPipelineSpec>());

    // Round-trips a keeper-produced compressed view through the get-flow-view-v2 wire result and back:
    // wrap -> YSON over flow_execute -> parsed back -> decompressed on the client side.
    auto roundTrip = [] (const TCompressedFlowView& compressed) {
        TGetFlowViewV2Result wire;
        wire.Codec = compressed.Codec;
        wire.Data = std::string(compressed.Data.Begin(), compressed.Data.Size());
        return DecompressFlowView(ConvertTo<TGetFlowViewV2Result>(ConvertToYsonString(wire)));
    };

    auto plain = keeper->GetYsonString(/*cache*/ true);
    // The cached full view, served straight from the keeper's pre-compressed blob.
    EXPECT_EQ(roundTrip(keeper->GetCompressedYsonString()).ToString(), plain.ToString());
    // A freshly compressed view, as get-flow-view-v2 produces for a sub-path or cache=false request.
    EXPECT_EQ(roundTrip(keeper->CompressYson(plain)).ToString(), plain.ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
