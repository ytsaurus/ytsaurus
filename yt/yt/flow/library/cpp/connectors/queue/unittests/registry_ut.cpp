#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>
#include <yt/yt/flow/library/cpp/common/yt_path_option.h>

#include <yt/yt/flow/library/cpp/connectors/queue/spec.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TEST(TQueueRegistryTest, ProductionTypesAreRegistered)
{
    EXPECT_THAT(TRegistry::Get()->GetSourceTypeNames(), testing::Contains("NYT::NFlow::TQueueSource"));

    const auto sinkTypeNames = TRegistry::Get()->GetSinkTypeNames();
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::TAsyncQueueSink"));
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::TAsyncMultiClusterQueueSink"));
    EXPECT_THAT(sinkTypeNames, testing::Contains("NYT::NFlow::TSyncQueueSink"));
}

std::optional<EYTPathOwnership> FindQueuePathOwnership(const TYsonStructPtr& parameters)
{
    for (const auto& claim : TRegistry::Get()->CollectYTPathClaims(parameters)) {
        if (claim.ParameterKey == "queue_path") {
            return claim.Ownership;
        }
    }
    return std::nullopt;
}

TEST(TQueueRegistryTest, QueuePathOwnership)
{
    auto sourceSpec = ConvertTo<TSourceSpecPtr>(TYsonStringBuf(R"({
        source_class_name = "NYT::NFlow::TQueueSource";
        parameters = {
            queue_path = "<cluster=primary>//queue";
            consumer_path = "<cluster=primary>//consumer";
        };
    })"));
    EXPECT_EQ(
        FindQueuePathOwnership(TRegistry::Get()->ParseSourceParameters(sourceSpec)),
        EYTPathOwnership::ReadOnly);

    for (TStringBuf sinkClassName : {"NYT::NFlow::TAsyncQueueSink", "NYT::NFlow::TAsyncMultiClusterQueueSink", "NYT::NFlow::TSyncQueueSink"}) {
        auto sinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(Format(R"({
            sink_class_name = %Qv;
            input_stream_ids = [s];
            parameters = {
                queue_path = "<cluster=primary>//queue";
                producer_path = "<cluster=primary>//producer";
            };
        })",
            sinkClassName)));
        EXPECT_EQ(
                FindQueuePathOwnership(TRegistry::Get()->ParseSinkParameters(sinkSpec)),
                EYTPathOwnership::SharedWrite)
            << sinkClassName;
    }
}

TEST(TQueueRegistryTest, QueueInfoSpecAloneClaimsNoQueuePath)
{
    auto spec = ConvertTo<TQueueInfoSpecPtr>(TYsonStringBuf(R"({queue_path = "<cluster=primary>//queue"})"));
    EXPECT_EQ(FindQueuePathOwnership(spec), std::nullopt);
}

TEST(TQueueRegistryTest, TwoSinksIntoOneQueueAreValid)
{
    auto spec = ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(R"({
        computations = {
            c = {
                computation_class_name = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
                source_streams = {
                    source = {
                        source_class_name = "NYT::NFlow::TQueueSource";
                        parameters = {
                            queue_path = "<cluster=primary>//queue";
                            consumer_path = "<cluster=primary>//consumer";
                        };
                    };
                };
                output_stream_ids = [s];
                sinks = {
                    first = {
                        sink_class_name = "NYT::NFlow::TAsyncQueueSink";
                        input_stream_ids = [s];
                        parameters = {
                            queue_path = "<cluster=primary>//queue";
                            producer_path = "<cluster=primary>//producer";
                        };
                    };
                    second = {
                        sink_class_name = "NYT::NFlow::TAsyncQueueSink";
                        input_stream_ids = [s];
                        parameters = {
                            queue_path = "<cluster=primary>//queue";
                            producer_path = "<cluster=primary>//producer";
                        };
                    };
                };
            };
        };
        streams = {
            s = { schema = [ { name = key; type = string; } ]; };
        };
    })"));
    ValidatePipelineSpec(spec);
}

TPipelineSpecPtr MakeMultiClusterSinkAndStateManagerSpec(
    TStringBuf statePath,
    TStringBuf queuePath = "<clusters=[a;b]>//tmp/state")
{
    return ConvertTo<TPipelineSpecPtr>(TYsonStringBuf(Format(R"({
        computations = {
            c = {
                computation_class_name = "NYT::NFlow::TSwiftPassthroughOrderedSourceComputation";
                source_streams = {
                    source = {
                        source_class_name = "NYT::NFlow::TQueueSource";
                        parameters = {
                            queue_path = "<cluster=primary>//queue";
                            consumer_path = "<cluster=primary>//consumer";
                        };
                    };
                };
                output_stream_ids = [s];
                sinks = {
                    sink = {
                        sink_class_name = "NYT::NFlow::TAsyncMultiClusterQueueSink";
                        input_stream_ids = [s];
                        parameters = {
                            use_clusters = %%true;
                            queue_path = %Qv;
                            producer_path = "<clusters=[a;b]>//tmp/producer";
                        };
                    };
                };
            };
            m = {
                computation_class_name = "NYT::NFlow::TPassthroughComputation";
                group_by_schema = [ { name = key; type = string; sort_order = ascending; } ];
                input_stream_ids = [s];
                output_stream_ids = [t];
                external_state_managers = {
                    "/state" = {
                        external_state_manager_class_name = "NYT::NFlow::TSimpleExternalStateManager";
                        parameters = { path = %Qv; };
                    };
                };
            };
        };
        streams = {
            s = { schema = [ { name = key; type = string; } ]; };
            t = { schema = [ { name = key; type = string; } ]; };
        };
    })",
        queuePath,
        statePath)));
}

TEST(TQueueRegistryTest, MultiClusterSinkClaimsItsClustersOnly)
{
    auto disjointSpec = MakeMultiClusterSinkAndStateManagerSpec("//tmp/state");
    ValidatePipelineSpec(disjointSpec, "c");
    ValidatePipelineSpec(MakeMultiClusterSinkAndStateManagerSpec("//tmp/producer"), "c");

    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(disjointSpec, "a"); },
        "claimed for writing");
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(MakeMultiClusterSinkAndStateManagerSpec("<cluster=b>//tmp/state"), "c"); },
        "claimed for writing");
}

TEST(TQueueRegistryTest, MultiClusterSinkClaimsUnionOfClusterAttributes)
{
    auto collectQueueClusters = [] (TStringBuf queuePath) {
        auto sinkSpec = ConvertTo<TSinkSpecPtr>(TYsonStringBuf(Format(R"({
            sink_class_name = "NYT::NFlow::TAsyncMultiClusterQueueSink";
            input_stream_ids = [s];
            parameters = {
                use_clusters = %%true;
                queue_path = %Qv;
                producer_path = "<clusters=[a;b]>//tmp/producer";
            };
        })",
            queuePath)));
        std::vector<std::string> clusters;
        for (const auto& claim : TRegistry::Get()->CollectYTPathClaims(TRegistry::Get()->ParseSinkParameters(sinkSpec), "c")) {
            if (claim.ParameterKey == "queue_path") {
                clusters.push_back(claim.Cluster);
            }
        }
        return clusters;
    };
    EXPECT_EQ(collectQueueClusters("<cluster=a;clusters=[b]>//tmp/state"), (std::vector<std::string>{"a", "b"}));

    auto mixedQueuePath = "<cluster=a;clusters=[b]>//tmp/state";
    ValidatePipelineSpec(MakeMultiClusterSinkAndStateManagerSpec("//tmp/state", mixedQueuePath), "c");
    EXPECT_THROW_WITH_SUBSTRING(
        { ValidatePipelineSpec(MakeMultiClusterSinkAndStateManagerSpec("<cluster=b>//tmp/state", mixedQueuePath), "c"); },
        "claimed for writing");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
