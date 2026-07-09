#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TComputationSpecPtr ParseComputationSpec(TStringBuf yson)
{
    return ConvertTo<TComputationSpecPtr>(TYsonStringBuf(yson));
}

void ExpectValidationError(const TComputationSpecPtr& spec, TStringBuf substring)
{
    try {
        TRegistry::Get()->ValidateComputationSpec(spec);
        ADD_FAILURE() << "Expected a validation error containing " << substring;
    } catch (const std::exception& ex) {
        EXPECT_TRUE(TString(ex.what()).Contains(substring)) << ex.what();
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TSwiftMapSpecValidationTest, RejectsSourceStreams)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TSwiftPassthroughComputation";
            group_by_schema = [];
            input_stream_ids = [ in ];
            source_streams = {
                s = { source_class_name = "SomeSource"; };
            };
        }
    )##");
    ExpectValidationError(spec, "does not support source streams");
}

TEST(TSwiftMapSpecValidationTest, RejectsMissingInputStreams)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TSwiftPassthroughComputation";
            group_by_schema = [];
        }
    )##");
    ExpectValidationError(spec, "requires input streams");
}

TEST(TSwiftMapSpecValidationTest, AcceptsWellFormedSpec)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TSwiftPassthroughComputation";
            group_by_schema = [];
            input_stream_ids = [ in ];
        }
    )##");
    EXPECT_NO_THROW(TRegistry::Get()->ValidateComputationSpec(spec));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
