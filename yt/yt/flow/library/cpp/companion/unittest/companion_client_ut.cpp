#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/actions/future.h>

#include <yt/yt/flow/library/cpp/companion/companion_client.h>
#include <yt/yt/flow/library/cpp/companion/companion_client_detail.h>
#include <yt/yt/flow/library/cpp/companion/companion_model.h>
#include <yt/yt/flow/library/cpp/companion/proto/companion_service.pb.h>

#include <yt/yt/flow/library/cpp/common/resource.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/ytree/convert.h>

#include <library/cpp/yt/yson_string/convert.h>

namespace NYT::NFlow::NCompanion {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionClientTest, TestParseTCompanionStatus)
{
    auto result = NYT::NYTree::ConvertTo<TCompanionInfoPtr>(TYsonString(TStringBuf(R""""(
        {
            computations={
                "computation_id_1"={
                    computation_id="computation_id_1";
                    computation_type="Source";
                };
                "computation_id_2"={
                    computation_id="computation_id_2";
                    computation_type="Source";
                };
                "computation_id_3"={
                    computation_id="computation_id_3";
                    computation_type="Transform";
                };
                "computation_id_4"={
                    computation_id="computation_id_4";
                    computation_type="Source";
                };
            };
        }
    )"""")));
    auto computation1 = result->Computations[TComputationId("computation_id_1")];
    EXPECT_EQ(ECompanionComputationType::Source, computation1->CompanionComputationType);
    auto computation2 = result->Computations[TComputationId("computation_id_2")];
    EXPECT_EQ(ECompanionComputationType::Source, computation2->CompanionComputationType);
    auto computation3 = result->Computations[TComputationId("computation_id_3")];
    EXPECT_EQ(ECompanionComputationType::Transform, computation3->CompanionComputationType);
    auto computation4 = result->Computations[TComputationId("computation_id_4")];
    EXPECT_EQ(ECompanionComputationType::Source, computation4->CompanionComputationType);
}

TEST(TCompanionClientTest, RemoveJobIsSingleAttempt)
{
    // No companion behind the address: the single-attempt best-effort call
    // must resolve with an error instead of retrying or hanging.
    auto client = New<TCompanionClient>(
        "localhost:1",
        /*timeout*/ TDuration::Seconds(5),
        TExponentialBackoffOptions{},
        /*statusProfiler*/ nullptr);

    auto error = client->RemoveJob(TJobId(TGuid::Create())).BlockingGet();
    EXPECT_FALSE(error.IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCompanionResourceModelTest, TestInitResourceCommandArgRoundTrip)
{
    TInitResourceCommandArg arg;
    arg.IncarnationId = TResourceInstanceId(TGuid::Create());
    arg.IncarnationGeneration = 5;
    arg.ConfigurationGeneration = 17;
    arg.Spec = ConvertTo<TResourceSpecPtr>(TYsonString(TStringBuf(R""""(
        {
            resource_class_name="NYT::NFlow::NCompanion::TCompanionResource";
            parameters={
                companion_resource_class="com.example.MyResource";
            };
        }
    )"""")));
    arg.DynamicSpec = ConvertTo<TDynamicResourceSpecPtr>(TYsonString(TStringBuf(R""""(
        {
            parameters={
                threshold=42;
            };
        }
    )"""")));
    TCompanionResourceInstanceReference dependency;
    dependency.ResourceId = "dependency";
    dependency.IncarnationId = TResourceInstanceId(TGuid::Create());
    dependency.ConfigurationGeneration = 9;
    dependency.Alias = TResourceId("Dictionary");
    arg.Dependencies.push_back(dependency);
    arg.ResourceRevision = New<TResourceRevision>();
    arg.ResourceRevision->RevisionId = 11;
    arg.ResourceRevision->Spec = ConvertToNode(TYsonString(TStringBuf("{path=\"prepared\"}")));

    auto parsed = ConvertTo<TInitResourceCommandArg>(ConvertToYsonString(arg));
    EXPECT_EQ(arg.IncarnationId, parsed.IncarnationId);
    EXPECT_EQ(5u, parsed.IncarnationGeneration);
    EXPECT_EQ(17u, parsed.ConfigurationGeneration);
    ASSERT_EQ(1u, parsed.Dependencies.size());
    EXPECT_EQ(TResourceId("dependency"), parsed.Dependencies[0].ResourceId);
    EXPECT_EQ(dependency.IncarnationId, parsed.Dependencies[0].IncarnationId);
    EXPECT_EQ(9u, parsed.Dependencies[0].ConfigurationGeneration);
    ASSERT_TRUE(parsed.Dependencies[0].Alias);
    EXPECT_EQ(TResourceId("Dictionary"), *parsed.Dependencies[0].Alias);
    EXPECT_EQ(
        "NYT::NFlow::NCompanion::TCompanionResource",
        parsed.Spec->ResourceClassName);
    EXPECT_EQ(
        "com.example.MyResource",
        parsed.Spec->Parameters->GetChildValueOrThrow<std::string>("companion_resource_class"));
    EXPECT_EQ(42, parsed.DynamicSpec->Parameters->GetChildValueOrThrow<i64>("threshold"));
    ASSERT_TRUE(parsed.ResourceRevision);
    EXPECT_EQ(11, parsed.ResourceRevision->RevisionId);
    EXPECT_EQ(
        "prepared",
        parsed.ResourceRevision->Spec->AsMap()->GetChildValueOrThrow<std::string>("path"));
}

TEST(TCompanionResourceModelTest, TestInitResourceCommandArgThrowsOnUnrecognized)
{
    EXPECT_THROW(
        ConvertTo<TInitResourceCommandArg>(TYsonString(TStringBuf(R""""(
            {
                spec={resource_class_name="C"};
                dynamic_spec={};
                unexpected_field=1;
            }
        )""""))),
        std::exception);
}

TEST(TCompanionResourceModelTest, TestLifecycleArgumentsUseRegisteredDefaults)
{
    auto init = ConvertTo<TInitResourceCommandArg>(TYsonString(TStringBuf(R""""(
        {
            spec={resource_class_name="C"};
            dynamic_spec={};
            incarnation_id="1-2-3-4";
        }
    )"""")));
    EXPECT_EQ(0u, init.IncarnationGeneration);
    EXPECT_EQ(0u, init.ConfigurationGeneration);
    EXPECT_FALSE(init.ResourceRevision);
}

TEST(TCompanionResourceModelTest, TestResourceReferenceProtoRoundTrip)
{
    TCompanionResourceInstanceReference reference;
    reference.ResourceId = "resource";
    reference.IncarnationId = TResourceInstanceId(TGuid::Create());
    reference.ConfigurationGeneration = 7;
    reference.Alias = TResourceId("alias");

    NProto::NCompanion::TCompanionResourceInstanceReference protoReference;
    ToProto(&protoReference, reference);
    TCompanionResourceInstanceReference parsed;
    FromProto(&parsed, protoReference);

    EXPECT_EQ(reference, parsed);
}

TEST(TCompanionResourceModelTest, TestUnloadResourceCommandArgRoundTrip)
{
    TUnloadResourceCommandArg arg;
    arg.IncarnationId = TResourceInstanceId(TGuid::Create());

    auto parsed = ConvertTo<TUnloadResourceCommandArg>(ConvertToYsonString(arg));
    EXPECT_EQ(arg.IncarnationId, parsed.IncarnationId);
}

TEST(TCompanionResourceModelTest, TestResourceReferenceThrowsOnUnrecognized)
{
    EXPECT_THROW(
        ConvertTo<TCompanionResourceInstanceReference>(TYsonString(TStringBuf(R""""(
            {
                resource_id="resource";
                incarnation_id="1-2-3-4";
                configuration_generation=1;
                unexpected_field=1;
            }
        )""""))),
        std::exception);
}

} // namespace
} // namespace NYT::NFlow::NCompanion
