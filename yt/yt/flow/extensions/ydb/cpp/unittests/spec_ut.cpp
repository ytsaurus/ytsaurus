#include <yt/yt/flow/extensions/ydb/cpp/options.h>
#include <yt/yt/flow/extensions/ydb/cpp/sink.h>
#include <yt/yt/flow/extensions/ydb/cpp/spec.h>
#include <yt/yt/flow/extensions/ydb/cpp/two_phase_commit_sink.h>

#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NFlow {
namespace {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr MakeAddress()
{
    return BuildYsonNodeFluently()
        .BeginMap()
        .Item("endpoint")
        .Value("localhost:2135")
        .Item("database")
        .Value("/Root/database")
        .Item("topic")
        .Value("folder/topic")
        .Item("payload_column")
        .Value("payload")
        .EndMap()
        ->AsMap();
}

TEST(TYdbSpecTest, AddressFieldsAreRequiredAndNonEmpty)
{
    for (const auto& field : {"endpoint", "database", "topic"}) {
        auto missing = MakeAddress();
        missing->RemoveChild(field);
        EXPECT_THROW(ConvertTo<TYdbSinkParametersPtr>(missing), std::exception) << field;

        auto empty = MakeAddress();
        empty->RemoveChild(field);
        empty->AddChild(field, ConvertToNode(""));
        EXPECT_THROW(ConvertTo<TYdbSinkParametersPtr>(empty), std::exception) << field;
    }
}

TEST(TYdbSpecTest, SinkDefaults)
{
    auto parameters = ConvertTo<TYdbSinkParametersPtr>(MakeAddress());
    EXPECT_EQ(parameters->MaxMemoryUsageBytes, 50_MB);
    EXPECT_EQ(parameters->Codec, "zstd");
    EXPECT_EQ(parameters->CompressionLevel, 4);
    EXPECT_EQ(parameters->ClientControllerTimeout, TDuration::Seconds(30));
    EXPECT_EQ(parameters->UpdatePartitionCountPeriod, TDuration::Seconds(60));
}

TEST(TYdbSpecTest, SinkRejectsUnsupportedCodec)
{
    auto spec = MakeAddress();
    spec->AddChild("codec", ConvertToNode("unsupported"));
    EXPECT_THROW_WITH_SUBSTRING(ConvertTo<TYdbSinkParametersPtr>(spec), "Unsupported codec");
}

TEST(TYdbSpecTest, SinkOptionsKeepRawCodecAndConfiguredAddress)
{
    auto spec = MakeAddress();
    spec->AddChild("codec", ConvertToNode("gzip"));
    auto parameters = ConvertTo<TYdbSinkParametersPtr>(spec);
    auto options = MakeYdbSinkOptions(*parameters, std::nullopt);
    EXPECT_EQ(options->Codec, "gzip");
    EXPECT_EQ(options->Endpoint, "localhost:2135");
    EXPECT_EQ(options->Topology.Database, "/Root/database");
    EXPECT_EQ(options->Topology.Topic, "folder/topic");
    ASSERT_EQ(options->Topology.DiscoveryEntries.size(), 1u);
    EXPECT_EQ(options->Topology.DiscoveryEntries.front(), (TYdbWriteTarget{.Endpoint = "localhost:2135", .Database = "/Root/database"}));
    EXPECT_FALSE(options->FramingFormat);
}

TEST(TYdbSpecTest, SourceDynamicOptionsCopyEveryField)
{
    auto parameters = ConvertTo<TDynamicYdbSourceParametersPtr>(BuildYsonNodeFluently()
            .BeginMap()
            .Item("malformed_message_policy")
            .Value("drop")
            .Item("max_time_lag")
            .Value(TDuration::Seconds(5))
            .Item("commit_timeout")
            .Value(TDuration::Seconds(7))
            .Item("session_silence_timeout")
            .Value(TDuration::Seconds(11))
            .EndMap());
    auto options = MakeYdbSourceDynamicOptions(*parameters);
    EXPECT_EQ(options->MalformedMessagePolicy, EMalformedYdbMessagePolicy::Drop);
    EXPECT_EQ(options->MaxTimeLag, TDuration::Seconds(5));
    EXPECT_EQ(options->CommitTimeout, TDuration::Seconds(7));
    EXPECT_EQ(options->SessionSilenceTimeout, TDuration::Seconds(11));
    EXPECT_FALSE(options->ReadFromInstant);
}

std::shared_ptr<const TYdbSourceDynamicOptions> MakeSourceDynamicOptionsFromYson(const INodePtr& node)
{
    return MakeYdbSourceDynamicOptions(*ConvertTo<TDynamicYdbSourceParametersPtr>(node));
}

TEST(TYdbSpecTest, EveryAcceptedSourceDynamicSettingChangesTheRestartToken)
{
    auto unchanged = MakeSourceDynamicOptionsFromYson(BuildYsonNodeFluently().BeginMap().EndMap());
    EXPECT_TRUE(AreNodesEqual(
        unchanged->RestartToken,
        MakeSourceDynamicOptionsFromYson(BuildYsonNodeFluently().BeginMap().EndMap())->RestartToken));

    const std::vector<std::pair<std::string, INodePtr>> changes = {
        {"malformed_message_policy", ConvertToNode("drop")},
        {"read_from_instant", ConvertToNode(TInstant::Seconds(1700000000))},
        {"max_time_lag", ConvertToNode(TDuration::Seconds(5))},
        {"commit_timeout", ConvertToNode(TDuration::Seconds(7))},
        {"session_silence_timeout", ConvertToNode(TDuration::Seconds(11))},
        {"unavailable_threshold", ConvertToNode(TDuration::Seconds(13))},
    };
    for (const auto& [name, value] : changes) {
        auto changed = MakeSourceDynamicOptionsFromYson(BuildYsonNodeFluently()
                .BeginMap()
                .Item(name)
                .Value(value)
                .EndMap());
        EXPECT_FALSE(AreNodesEqual(unchanged->RestartToken, changed->RestartToken)) << name;
    }
}

TEST(TYdbSpecTest, TwoPhaseCommitOptionsCarryPublicDiscriminator)
{
    auto parameters = ConvertTo<TYdbTwoPhaseCommitSinkParametersPtr>(MakeAddress());
    EXPECT_EQ(MakeYdbTwoPhaseCommitOptions(*parameters)->WriterIdentityType, "ytflow-ydb-two-phase-commit");
}

TEST(TYdbSpecTest, TwoPhaseCommitDynamicOptionsCopyBatching)
{
    auto parameters = ConvertTo<TDynamicYdbTwoPhaseCommitSinkParametersPtr>(BuildYsonNodeFluently()
            .BeginMap()
            .Item("max_rows_per_batch")
            .Value(3)
            .Item("max_bytes_per_batch")
            .Value(4096)
            .EndMap());
    auto options = MakeYdbTwoPhaseCommitDynamicOptions(*parameters);
    EXPECT_EQ(options->Batching.MaxRowsPerBatch, 3);
    EXPECT_EQ(options->Batching.MaxBytesPerBatch, 4096);
}

////////////////////////////////////////////////////////////////////////////////

template <class TUnited, class TGeneric, class TSinkParameters, class TControllerParameters>
void ExpectRegisteredParametersShareGenericBase(const TYsonStructPtr& registered)
{
    ASSERT_TRUE(registered);
    auto* united = dynamic_cast<TUnited*>(registered.Get());
    ASSERT_TRUE(united);
    TGeneric* uniqueGenericBase = united;

    auto generic = DynamicPointerCast<TGeneric>(registered);
    auto sinkParameters = DynamicPointerCast<TSinkParameters>(registered);
    auto controllerParameters = DynamicPointerCast<TControllerParameters>(registered);
    ASSERT_TRUE(generic);
    ASSERT_TRUE(sinkParameters);
    ASSERT_TRUE(controllerParameters);

    EXPECT_EQ(generic.Get(), uniqueGenericBase);
    EXPECT_EQ(static_cast<TYsonStruct*>(generic.Get()), registered.Get());
    EXPECT_EQ(DynamicPointerCast<TSinkParameters>(generic).Get(), sinkParameters.Get());
    EXPECT_EQ(DynamicPointerCast<TControllerParameters>(generic).Get(), controllerParameters.Get());
    EXPECT_EQ(DynamicPointerCast<TGeneric>(sinkParameters).Get(), generic.Get());
    EXPECT_EQ(DynamicPointerCast<TGeneric>(controllerParameters).Get(), generic.Get());
}

template <class TSink>
TSinkSpecPtr MakeRegisteredSinkSpec()
{
    auto spec = New<TSinkSpec>();
    spec->SinkClassName = TypeName<TSink>();
    spec->Parameters = MakeAddress();
    return spec;
}

template <class TSink>
class TRegisteredYdbSinkParametersTest
    : public testing::Test
{ };

using TRegisteredYdbSinks = testing::Types<
    TYdbSink,
    TYdbFramingSink,
    TYdbTwoPhaseCommitSink,
    TYdbAtLeastOnceSink,
    TYdbAtLeastOnceFramingSink>;

TYPED_TEST_SUITE(TRegisteredYdbSinkParametersTest, TRegisteredYdbSinks);

TYPED_TEST(TRegisteredYdbSinkParametersTest, StaticParametersShareGenericBase)
{
    using TSinkParameters = typename TypeParam::TParameters;
    using TControllerParameters = typename TypeParam::TSinkController::TParameters;

    auto registered = TRegistry::Get()->ParseSinkParameters(MakeRegisteredSinkSpec<TypeParam>());
    ASSERT_NO_FATAL_FAILURE((ExpectRegisteredParametersShareGenericBase<
        TUnitedParameters<TypeParam>,
        ISink::TParameters,
        TSinkParameters,
        TControllerParameters>(registered)));

    EXPECT_EQ(DynamicPointerCast<TSinkParameters>(registered)->Topic, "folder/topic");
    EXPECT_EQ(DynamicPointerCast<TControllerParameters>(registered)->Topic, "folder/topic");
    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(registered),
        ConvertToNode(ConvertTo<TIntrusivePtr<TSinkParameters>>(MakeAddress()))));
}

TYPED_TEST(TRegisteredYdbSinkParametersTest, DynamicParametersShareGenericBase)
{
    using TSinkParameters = typename TypeParam::TDynamicParameters;
    using TControllerParameters = typename TypeParam::TSinkController::TDynamicParameters;

    auto heartbeatPeriodNode = BuildYsonNodeFluently()
        .BeginMap()
        .Item("flow_queue_meta_heartbeat_period")
        .Value(TDuration::Seconds(17))
        .EndMap()
        ->AsMap();
    auto dynamicSpec = New<TDynamicSinkSpec>();
    dynamicSpec->Parameters = heartbeatPeriodNode;

    auto registered = TRegistry::Get()->ParseDynamicSinkParameters(MakeRegisteredSinkSpec<TypeParam>(), dynamicSpec);
    ASSERT_NO_FATAL_FAILURE((ExpectRegisteredParametersShareGenericBase<
        TDynamicUnitedParameters<TypeParam>,
        ISink::TDynamicParameters,
        TSinkParameters,
        TControllerParameters>(registered)));

    EXPECT_EQ(DynamicPointerCast<TSinkParameters>(registered)->FlowQueueMetaHeartbeatPeriod, TDuration::Seconds(17));
    EXPECT_EQ(DynamicPointerCast<TControllerParameters>(registered)->FlowQueueMetaHeartbeatPeriod, TDuration::Seconds(17));
    EXPECT_TRUE(AreNodesEqual(
        ConvertToNode(registered),
        ConvertToNode(ConvertTo<TIntrusivePtr<TSinkParameters>>(heartbeatPeriodNode))));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
