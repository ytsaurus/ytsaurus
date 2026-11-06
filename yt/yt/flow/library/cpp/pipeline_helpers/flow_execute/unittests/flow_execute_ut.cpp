#include <yt/yt/flow/library/cpp/pipeline_helpers/flow_execute/flow_execute.h>

#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TFlowExecuteTest, DynamicSpecPatchRequiresCasAndNonemptyComment)
{
    for (auto argument : {
            R"({set_path="/x";set_value=1;comment="reason";})",
            R"({expected_version=1;set_path="/x";set_value=1;})",
            R"({expected_version=1;set_path="/x";set_value=1;comment="";})",
         }) {
        EXPECT_THROW(ConvertTo<TPatchPipelineDynamicSpecArg>(TYsonStringBuf(argument)), TErrorException);
    }
    auto argument = ConvertTo<TPatchPipelineDynamicSpecArg>(TYsonStringBuf(
        "{expected_version=1;set_path=\"/x\";set_value=1;comment=\"  Причина 😀  \";}"));
    EXPECT_EQ(argument.Comment, "  Причина 😀  ");
    auto multiPath = ConvertTo<TPatchPipelineDynamicSpecArg>(TYsonStringBuf(
        R"({expected_version=1;set_changes=[{path="/x";value=1;};{path="/y";value=2;};];comment="reason";})"));
    ASSERT_TRUE(multiPath.SetChanges);
    EXPECT_EQ(multiPath.SetChanges->size(), 2u);
    EXPECT_EQ((*multiPath.SetChanges)[1].Path, "/y");
    EXPECT_THROW(ConvertTo<TPatchPipelineDynamicSpecArg>(TYsonStringBuf(
        R"({expected_version=1;set_changes=[];comment="reason";})")),
        TErrorException);
    EXPECT_THROW(ConvertTo<TPatchPipelineDynamicSpecArg>(TYsonStringBuf(
        R"({expected_version=1;set_path="/x";set_value=1;set_changes=[{path="/y";value=2;};];comment="reason";})")),
        TErrorException);
    auto commentUpdate = ConvertTo<TPatchPipelineDynamicSpecArg>(TYsonStringBuf(
        "{expected_version=2;patch=1;comment=\"Новая причина\";}"));
    EXPECT_EQ(commentUpdate.Patch, TVersion(1));
    EXPECT_FALSE(commentUpdate.SetPath);
    EXPECT_THROW(ConvertTo<TPatchPipelineDynamicSpecArg>(TYsonStringBuf(
        "{expected_version=2;remove_path=\"/x\";comment=\"reason\";}")),
        TErrorException);
    EXPECT_NO_THROW(ConvertTo<TSetPipelineDynamicSpecArg>(TYsonStringBuf("{spec=1;}")));
    EXPECT_NO_THROW(ConvertTo<TRemovePipelineDynamicSpecArg>(TYsonStringBuf("{path=\"/x\";}")));
    EXPECT_THROW(ConvertTo<TResetPipelineDynamicSpecArg>(TYsonStringBuf("{}")), TErrorException);
}

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
