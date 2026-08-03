#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/computation/transform_ordered_source_computation.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {

class TValidationProbeTransformOrderedSourceComputation
    : public TTransformOrderedSourceComputation
{
public:
    using TTransformOrderedSourceComputation::TTransformOrderedSourceComputation;
};

YT_FLOW_DEFINE_COMPUTATION(TValidationProbeTransformOrderedSourceComputation);

namespace {

using namespace NYson;
using namespace NYTree;

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

TEST(TTransformOrderedSourceValidationTest, RejectsInputStreams)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            input_stream_ids = [ in ];
        }
    )##");
    ExpectValidationError(spec, "does not support input streams");
}

TEST(TTransformOrderedSourceValidationTest, RejectsTimerStreams)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            timer_streams = { t = {}; };
        }
    )##");
    ExpectValidationError(spec, "does not support timers");
}

TEST(TTransformOrderedSourceValidationTest, RejectsKeyVisitorStreams)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            key_visitor_streams = { kv = {}; };
        }
    )##");
    ExpectValidationError(spec, "does not support key_visitor_streams");
}

TEST(TTransformOrderedSourceValidationTest, RejectsGroupBySchema)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            group_by_schema = [ {name = k; type = uint64; required = %true;} ];
        }
    )##");
    ExpectValidationError(spec, "does not support group_by_schema");
}

TEST(TTransformOrderedSourceValidationTest, RejectsMissingSource)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
        }
    )##");
    ExpectValidationError(spec, "requires exactly one source stream");
}

TEST(TTransformOrderedSourceValidationTest, RejectsMultipleSources)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { first = {}; second = {}; };
        }
    )##");
    ExpectValidationError(spec, "requires exactly one source stream");
}

TEST(TTransformOrderedSourceValidationTest, RejectsExternalStateManagers)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            external_state_managers = { esm = {}; };
        }
    )##");
    ExpectValidationError(spec, "does not support external_state_managers");
}

TEST(TTransformOrderedSourceValidationTest, AcceptsWatermarkStrategy)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            watermark_strategy = {
                watermark_generator = {};
                watermark_alignment = { group_name = g; };
            };
        }
    )##");
    EXPECT_NO_THROW(TRegistry::Get()->ValidateComputationSpec(spec));
}

TEST(TTransformOrderedSourceValidationTest, AcceptsMultipleOutputStreams)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            output_stream_ids = [ out_a; out_b ];
        }
    )##");
    EXPECT_NO_THROW(TRegistry::Get()->ValidateComputationSpec(spec));
}

TEST(TTransformOrderedSourceValidationTest, RejectsExternalStateJoinerWithoutKeySchemaOverride)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            external_state_joiners = { esj = {}; };
        }
    )##");
    ExpectValidationError(spec, "without join_on/key_schema_override");
}

TEST(TTransformOrderedSourceValidationTest, RejectsExternalStateJoinerWithKeyProviderStreamsOnly)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            external_state_joiners = {
                esj = { join_on = { key_provider_streams = [ queue ]; }; };
            };
        }
    )##");
    ExpectValidationError(spec, "without join_on/key_schema_override");
}

TEST(TTransformOrderedSourceValidationTest, AcceptsExternalStateJoinerWithKeySchemaOverride)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TValidationProbeTransformOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            external_state_joiners = {
                esj = {
                    join_on = {
                        key_schema_override = [ {name = k; type = uint64; required = %true;} ];
                        key_provider_streams = [ queue ];
                    };
                };
            };
        }
    )##");
    EXPECT_NO_THROW(TRegistry::Get()->ValidateComputationSpec(spec));
}

TEST(TSwiftOrderedSourceValidationTest, RejectsExternalStateManagers)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            external_state_managers = { esm = {}; };
        }
    )##");
    ExpectValidationError(spec, "does not support external_state_managers");
}

TEST(TSwiftOrderedSourceValidationTest, RejectsExternalStateJoinerWithKeyProviderStreamsOnly)
{
    auto spec = ParseComputationSpec(R"##(
        {
            computation_class_name = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
            source_streams = { queue = { source_class_name = "NYT::NFlow::TRandomSource"; }; };
            external_state_joiners = {
                esj = { join_on = { key_provider_streams = [ queue ]; }; };
            };
        }
    )##");
    ExpectValidationError(spec, "without join_on/key_schema_override");
}

} // namespace
} // namespace NYT::NFlow
