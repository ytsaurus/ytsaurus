#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/companion/server/job.h>

#include <yt/yt/core/misc/guid.h>

namespace NYT::NFlow::NCompanionServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::NCompanion::TJobInfo BuildJobInfo(
    const TResourceInstanceId& directIncarnationId = TResourceInstanceId(TGuid::Create()),
    const TResourceInstanceId& transitiveIncarnationId = TResourceInstanceId(TGuid::Create()))
{
    NProto::NCompanion::TJobInfo jobInfo;
    jobInfo.set_spec(R"({
        computation_class_name = "NYT::NFlow::NCompanion::TTransformCompanionComputation";
        processing_function = "TTestFunction";
        parameters = {
            internal_states = ["count_state"; "sum_state"];
        };
        external_state_managers = {
            geo_dict = {};
        };
        external_state_joiners = {
            user_profile = {};
        };
    })");
    jobInfo.set_dynamic_spec("{}");

    auto* inputStream = jobInfo.add_streams();
    inputStream->set_stream_id("input");
    inputStream->set_stream_spec_id(5);
    inputStream->set_schema(R"([{name = "user"; type = "string"}])");

    auto* outputStream = jobInfo.add_streams();
    outputStream->set_stream_id("output");
    outputStream->set_stream_spec_id(7);
    outputStream->set_schema(R"([{name = "count"; type = "uint64"}])");

    auto* directReference = jobInfo.add_companion_resources();
    directReference->set_resource_id("dictionary");
    ToProto(directReference->mutable_incarnation_id(), directIncarnationId.Underlying());
    directReference->set_configuration_generation(3);
    directReference->set_alias("geo");

    auto* transitiveReference = jobInfo.add_companion_resources();
    transitiveReference->set_resource_id("credentials");
    ToProto(transitiveReference->mutable_incarnation_id(), transitiveIncarnationId.Underlying());
    transitiveReference->set_configuration_generation(7);

    return jobInfo;
}

TEST(TJobTest, ParsesJobInfo)
{
    auto jobId = TJobId(TGuid::Create());
    auto directIncarnationId = TResourceInstanceId(TGuid::Create());
    auto transitiveIncarnationId = TResourceInstanceId(TGuid::Create());
    auto job = New<TJob>(
        jobId,
        TComputationId("my_computation"),
        BuildJobInfo(directIncarnationId, transitiveIncarnationId));

    EXPECT_EQ(job->GetJobId(), jobId);
    EXPECT_EQ(job->GetComputationId(), TComputationId("my_computation"));

    EXPECT_EQ(
        job->GetSpec()->ComputationClassName,
        "NYT::NFlow::NCompanion::TTransformCompanionComputation");
    ASSERT_TRUE(job->GetSpec()->ProcessingFunction.has_value());
    EXPECT_EQ(*job->GetSpec()->ProcessingFunction, "TTestFunction");

    EXPECT_EQ(
        job->GetInternalStateNames(),
        (THashSet<std::string>{"count_state", "sum_state"}));
    EXPECT_EQ(job->GetExternalStateNames(), (THashSet<std::string>{"geo_dict"}));
    EXPECT_EQ(job->GetJoinedStateNames(), (THashSet<std::string>{"user_profile"}));

    const auto& streamSpecs = job->GetStreamSpecs();
    EXPECT_EQ(streamSpecs->GetLastSpecId(TStreamId("input")), TStreamSpecId(5));
    EXPECT_EQ(streamSpecs->GetLastSpecId(TStreamId("output")), TStreamSpecId(7));
    EXPECT_EQ(streamSpecs->GetStreamId(TStreamSpecId(5)), TStreamId("input"));
    EXPECT_EQ(streamSpecs->GetSchema(TStreamSpecId(5))->GetColumnCount(), 1);
    EXPECT_EQ(
        streamSpecs->GetSchema(TStreamSpecId(7))->Columns()[0].Name(),
        "count");

    ASSERT_EQ(std::ssize(job->GetCompanionResources()), 2);
    EXPECT_EQ(job->GetCompanionResources()[0].ResourceId, TResourceId("dictionary"));
    EXPECT_EQ(job->GetCompanionResources()[0].IncarnationId, directIncarnationId);
    EXPECT_EQ(job->GetCompanionResources()[0].ConfigurationGeneration, 3);
    EXPECT_EQ(job->GetCompanionResources()[0].Alias, TResourceId("geo"));
    EXPECT_EQ(job->GetCompanionResources()[1].ResourceId, TResourceId("credentials"));
    EXPECT_EQ(job->GetCompanionResources()[1].IncarnationId, transitiveIncarnationId);
    EXPECT_FALSE(job->GetCompanionResources()[1].Alias);
}

TEST(TJobTest, EmptyStateDeclarations)
{
    NProto::NCompanion::TJobInfo jobInfo;
    jobInfo.set_spec(R"({computation_class_name = "Shim"})");
    jobInfo.set_dynamic_spec("{}");

    auto job = New<TJob>(TJobId(TGuid::Create()), TComputationId("c"), jobInfo);
    EXPECT_TRUE(job->GetInternalStateNames().empty());
    EXPECT_TRUE(job->GetExternalStateNames().empty());
    EXPECT_TRUE(job->GetJoinedStateNames().empty());
}

TEST(TStreamSpecCacheTest, ReusesSpecsAcrossBatches)
{
    google::protobuf::RepeatedPtrField<NProto::NCompanion::TStream> streams;
    auto* stream = streams.Add();
    stream->set_stream_id("data");
    stream->set_stream_spec_id(5);
    stream->set_schema(R"([{name = "value"; type = "string"}])");

    TStreamSpecCache cache;
    auto first = cache.Resolve(streams);
    auto second = cache.Resolve(streams);

    // The same (stream id, spec id) resolves to the same parsed spec, so the
    // schema pointer repeats and downstream pointer-keyed caches can hit.
    EXPECT_EQ(
        first->GetSchema(TStreamSpecId(5)).Get(),
        second->GetSchema(TStreamSpecId(5)).Get());

    // A new spec version parses fresh.
    stream->set_stream_spec_id(6);
    auto third = cache.Resolve(streams);
    EXPECT_NE(
        second->GetSchema(TStreamSpecId(5)).Get(),
        third->GetSchema(TStreamSpecId(6)).Get());
}

TEST(TStreamSpecCacheTest, ReparsesOnSchemaChange)
{
    google::protobuf::RepeatedPtrField<NProto::NCompanion::TStream> streams;
    auto* stream = streams.Add();
    stream->set_stream_id("data");
    stream->set_stream_spec_id(0);
    stream->set_schema(R"([{name = "value"; type = "string"}])");

    TStreamSpecCache cache;
    auto first = cache.Resolve(streams);

    // Override spec ids are positional and restart from zero every batch, so a
    // schema migration arrives under the same (stream id, spec id) key and
    // must not be served from the cache.
    stream->set_schema(R"([{name = "value"; type = "string"}; {name = "extra"; type = "int64"}])");
    auto second = cache.Resolve(streams);
    EXPECT_NE(
        first->GetSchema(TStreamSpecId(0)).Get(),
        second->GetSchema(TStreamSpecId(0)).Get());
    EXPECT_EQ(second->GetSchema(TStreamSpecId(0))->GetColumnCount(), 2);

    // Unchanged bytes still hit.
    auto third = cache.Resolve(streams);
    EXPECT_EQ(
        second->GetSchema(TStreamSpecId(0)).Get(),
        third->GetSchema(TStreamSpecId(0)).Get());
}

TEST(TJobTest, MalformedSpecThrows)
{
    NProto::NCompanion::TJobInfo jobInfo;
    jobInfo.set_spec("{computation_class_name = ");
    jobInfo.set_dynamic_spec("{}");

    EXPECT_THROW_WITH_SUBSTRING(
        Y_UNUSED(New<TJob>(TJobId(TGuid::Create()), TComputationId("c"), jobInfo)),
        "Failed to parse job specs");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NCompanionServer
