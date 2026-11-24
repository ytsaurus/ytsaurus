#include <yt/yt/flow/extensions/ydb/test/pipeline/delivery_validation.h>

#include <yt/yt/flow/library/cpp/process_function/testing/unittest.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/registry.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/guid.h>
#include <yt/yt/core/test_framework/framework.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow::NYdbTest {
namespace {

using namespace NTableClient;
using namespace NTesting;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

YT_FLOW_DEFINE_YSON_MESSAGE(TExpectedProbeRange);

TTableSchemaPtr LaneKeySchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        "[{name=hash;type=uint64;sort_order=ascending};"
        "{name=run_id;type=string;sort_order=ascending};"
        "{name=lane;type=string;sort_order=ascending}]")));
}

TKey LaneKey()
{
    return MakeKey(ui64(1), std::string("run"), std::string("lane"));
}

TTableSchemaPtr RawObservedSchema()
{
    return ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf(
        "[{name=run_id;type=string};{name=lane;type=string};{name=data;type=string};{name=unparsed;type=string}]")));
}

TInputMessageConstPtr LaneExpected(ui64 first, ui64 last, ui64 bucket = 100, std::string producer = "producer")
{
    return MakeTestMessage("expected", LaneKey(), GetYsonMessagePayloadSchema<TExpectedProbeRange>(), [&] (TMessageBuilder& builder) {
        builder.SetEventTimestamp(TSystemTimestamp(bucket));
        builder.Payload().Set(std::string("run"), "run_id");
        builder.Payload().Set(std::string("lane"), "lane");
        builder.Payload().Set(std::move(producer), "producer_id");
        builder.Payload().Set(bucket, "bucket_start");
        builder.Payload().Set(first, "first_sequence");
        builder.Payload().Set(last, "issued_through");
    });
}

TInputMessageConstPtr LaneObserved(ui64 sequence, ui64 bucket = 100, std::string producer = "producer")
{
    auto envelope = New<TDeliveryProbeEnvelope>();
    envelope->RunId = "run";
    envelope->Lane = "lane";
    envelope->ProducerId = std::move(producer);
    envelope->Sequence = sequence;
    envelope->ProbeTime = bucket;
    envelope->BucketStart = bucket;
    envelope->Data = Format("run/%v/%v", envelope->ProducerId, sequence);
    return MakeTestMessage("observed", LaneKey(), RawObservedSchema(), [&] (TMessageBuilder& builder) {
        builder.SetEventTimestamp(TSystemTimestamp(bucket));
        builder.Payload().Set(std::string("run"), "run_id");
        builder.Payload().Set(std::string("lane"), "lane");
        builder.Payload().Set(std::string(ConvertToYsonString(envelope).AsStringBuf()), "data");
    });
}

TInputTimerConstPtr LaneTimer(std::string stream = "close", ui64 bucket = 100)
{
    return MakeTestTimer(LaneKey(), TSystemTimestamp(bucket + 10), LaneKeySchema(), TStreamId(stream));
}

std::pair<std::unique_ptr<TProcessFunctionTestHarness>, IStatusProfilerPtr> MakeCombinedHarness(
    TTestStateEnvironment& stateEnv,
    bool exactlyOnce = false,
    ui64 expectedWatermark = 110,
    ui64 observedWatermark = 110)
{
    auto parameters = New<TDeliveryProbeCombinedValidatorParameters>();
    parameters->RunId = "run";
    parameters->Lane = "lane";
    parameters->WindowSize = TDuration::Seconds(10);
    parameters->ExactlyOnce = exactlyOnce;
    stateEnv.SetStaticParameters(parameters);
    auto statusProfiler = CreateSyncStatusProfiler();
    auto functionContext = New<TProcessFunctionContext>();
    functionContext->StatusProfiler = statusProfiler;
    auto spec = New<TComputationSpec>();
    spec->InputStreamIds = {TStreamId("expected"), TStreamId("observed")};
    auto context = TTestRuntimeContextBuilder()
        .SetKeySchema(LaneKeySchema())
        .SetSpec(std::move(spec))
        .SetCurrentTimestamp(TSystemTimestamp(200))
        .SetWatermark("expected", TSystemTimestamp(expectedWatermark))
        .SetWatermark("observed", TSystemTimestamp(observedWatermark))
        .Build();
    return {std::make_unique<TProcessFunctionTestHarness>(stateEnv, New<TDeliveryProbeValidator>(functionContext), context), statusProfiler};
}

TEST(TDeliveryProbeValidatorTest, NonzeroAnchorReorderLateAloAndBelowAnchor)
{
    TTestStateEnvironment stateEnv(LaneKeySchema());
    auto [harness, status] = MakeCombinedHarness(stateEnv);
    harness->RunEpoch({LaneObserved(9), LaneExpected(7, 10), LaneObserved(7), LaneObserved(8), LaneExpected(7, 10)}, {LaneTimer()});
    auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    ASSERT_TRUE(state);
    EXPECT_EQ(state->CheckedProbeCount, 3u);
    ASSERT_EQ(state->Producers.size(), 1u);
    EXPECT_EQ(state->Producers[0]->InitialSequence, 7u);
    EXPECT_EQ(state->Producers[0]->ClosedThrough, 10u);
    EXPECT_TRUE(state->Producers[0]->Windows.empty());
    harness->RunEpoch({}, {LaneTimer()});
    EXPECT_TRUE(harness->GetTimers().empty());
    harness->RunEpoch({LaneObserved(7), LaneExpected(7, 10)});
    state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->AllowedDuplicateCount, 1u);
    EXPECT_TRUE(state->FirstErrorKind.empty());
    harness->RunEpoch({LaneObserved(6)});
    state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->FirstErrorKind, "corrupt_probe");
    EXPECT_TRUE(status->GetStatus().Errors.empty());
}

TEST(TDeliveryProbeValidatorTest, MissingPrefixInteriorTailOrEntireWindowWaitForPairedClosure)
{
    for (const std::vector<ui64>& observed : {std::vector<ui64>{8, 9}, {7, 9}, {7, 8}, {}}) {
        TTestStateEnvironment stateEnv(LaneKeySchema());
        auto [harness, status] = MakeCombinedHarness(stateEnv, false, 110, 109);
        std::vector<TInputMessageConstPtr> messages{LaneExpected(7, 10)};
        for (auto sequence : observed) {
            messages.push_back(LaneObserved(sequence));
        }
        harness->RunEpoch(messages, {LaneTimer()});
        auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
        EXPECT_TRUE(state->FirstErrorKind.empty());
        EXPECT_EQ(state->Producers[0]->Windows.size(), 1u);
        auto input = New<TInputContext>(std::vector<TInputMessageConstPtr>{}, std::vector<TInputTimerConstPtr>{LaneTimer()});
        stateEnv.PreloadEpoch(input);
        auto context = TTestRuntimeContextBuilder()
            .SetKeySchema(LaneKeySchema())
            .SetWatermark("expected", TSystemTimestamp(110))
            .SetWatermark("observed", TSystemTimestamp(110))
            .Build();
        harness->GetFunction<TDeliveryProbeValidator>()->ProcessKey(input, New<TRecordingOutputCollector>(), context);
        stateEnv.CommitEpoch();
        state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
        EXPECT_EQ(state->FirstErrorKind, "delivery_loss");
        EXPECT_TRUE(state->Producers[0]->Windows.empty());
        EXPECT_TRUE(status->GetStatus().Errors.empty());
        harness->RunEpoch({}, {LaneTimer("status")});
        EXPECT_EQ(status->GetStatus().Errors.size(), 1u);
    }
}

TEST(TDeliveryProbeValidatorTest, FirstErrorPersistsAndReportsOnlyCommittedState)
{
    TTestStateEnvironment stateEnv(LaneKeySchema());
    auto [harness, status] = MakeCombinedHarness(stateEnv, true);
    harness->RunEpoch({LaneObserved(7), LaneObserved(7)}, {LaneTimer("status")});
    EXPECT_TRUE(status->GetStatus().Errors.empty());
    harness->RunEpoch({LaneExpected(1, 2)}, {LaneTimer("status")});
    auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->FirstErrorKind, "duplicate_exactly_once_probe");
    EXPECT_EQ(status->GetStatus().Errors.size(), 1u);
    EXPECT_EQ(harness->GetTimers().size(), 1u);
}

TEST(TDeliveryProbeValidatorTest, BoundedInventedProducersAndSequentialWindows)
{
    TTestStateEnvironment stateEnv(LaneKeySchema());
    auto [harness, status] = MakeCombinedHarness(stateEnv, false, 1000, 1000);
    for (ui64 index = 0; index < 40; ++index) {
        auto bucket = 100 + index * 10;
        harness->RunEpoch({LaneExpected(index + 7, index + 8, bucket), LaneObserved(index + 7, bucket)}, {LaneTimer("close", bucket)});
    }
    auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->CheckedProbeCount, 40u);
    EXPECT_TRUE(state->Producers[0]->Windows.empty());
    for (ui64 index = 0; index < 100; ++index) {
        harness->RunEpoch({LaneObserved(0, 500, Format("invented%v", index))});
    }
    state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->FirstErrorKind, "validation_state_limit");
    EXPECT_EQ(state->Producers.size(), 4u);
    for (const auto& producer : state->Producers) {
        EXPECT_TRUE(producer->Windows.empty());
    }
}

TEST(TDeliveryProbeValidatorTest, TwoProducersKeepIndependentNonzeroFrontiers)
{
    TTestStateEnvironment stateEnv(LaneKeySchema());
    auto [harness, status] = MakeCombinedHarness(stateEnv);
    harness->RunEpoch({LaneExpected(7, 8), LaneObserved(7), LaneExpected(30, 31, 100, "second"), LaneObserved(30, 100, "second")});
    harness->RunEpoch({}, {LaneTimer()});
    auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->CheckedProbeCount, 2u);
    ASSERT_EQ(state->Producers.size(), 2u);
    EXPECT_EQ(state->Producers[0]->ClosedThrough, 8u);
    EXPECT_EQ(state->Producers[1]->ClosedThrough, 31u);
    EXPECT_TRUE(state->FirstErrorKind.empty());
}

TEST(TDeliveryProbeValidatorTest, ExactlyOnceDuplicatesBeforeAndAfterClosure)
{
    for (bool afterClosure : {false, true}) {
        TTestStateEnvironment stateEnv(LaneKeySchema());
        auto [harness, status] = MakeCombinedHarness(stateEnv, true);
        harness->RunEpoch({LaneExpected(7, 8), LaneObserved(7)});
        if (afterClosure) {
            harness->RunEpoch({}, {LaneTimer()});
        }
        harness->RunEpoch({LaneObserved(7)});
        auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
        EXPECT_EQ(state->FirstErrorKind, "duplicate_exactly_once_probe");
        EXPECT_TRUE(state->Producers[0]->Windows.empty());
        EXPECT_TRUE(status->GetStatus().Errors.empty());
    }
}

TEST(TDeliveryProbeValidatorTest, ObservationOnlyAndMalformedInputsFailWithoutNewKeys)
{
    TTestStateEnvironment stateEnv(LaneKeySchema());
    auto [harness, status] = MakeCombinedHarness(stateEnv);
    harness->RunEpoch({LaneObserved(7)}, {LaneTimer()});
    auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->FirstErrorKind, "unexpected_observation");

    TTestStateEnvironment malformedEnv(LaneKeySchema());
    auto [malformedHarness, malformedStatus] = MakeCombinedHarness(malformedEnv);
    malformedHarness->RunEpoch({MakeTestMessage("observed", LaneKey(), RawObservedSchema(), [&] (TMessageBuilder& builder) {
        builder.Payload().Set(std::string("run"), "run_id");
        builder.Payload().Set(std::string("lane"), "lane");
        builder.Payload().Set(std::string("unframed"), "unparsed");
    })});
    state = malformedEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->FirstErrorKind, "malformed_probe");
    EXPECT_TRUE(state->Producers.empty());
}

TEST(TDeliveryProbeValidatorTest, OpenWindowAndSequenceCapsClearBulkyState)
{
    TTestStateEnvironment stateEnv(LaneKeySchema());
    auto [harness, status] = MakeCombinedHarness(stateEnv, false, 0, 0);
    for (ui64 index = 0; index < 31; ++index) {
        harness->RunEpoch({LaneExpected(index, index + 1, 100 + 10 * index)});
    }
    auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->FirstErrorKind, "validation_state_limit");
    EXPECT_TRUE(state->Producers[0]->Windows.empty());

    TTestStateEnvironment sequenceEnv(LaneKeySchema());
    auto [sequenceHarness, sequenceStatus] = MakeCombinedHarness(sequenceEnv, false, 0, 0);
    std::vector<TInputMessageConstPtr> observations;
    for (ui64 sequence = 0; sequence <= 20'000; ++sequence) {
        observations.push_back(LaneObserved(sequence));
    }
    sequenceHarness->RunEpoch(std::move(observations));
    state = sequenceEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
    EXPECT_EQ(state->FirstErrorKind, "validation_state_limit");
    EXPECT_TRUE(state->Producers[0]->Windows.empty());
}

TEST(TDeliveryProbeValidatorTest, EnvelopeCorruptionAndExpectedRangeLimitsPersistCommittedErrors)
{
    for (const auto& [mutation, errorKind] : std::vector<std::pair<std::string, std::string>>{
            {"null", "malformed_probe"},
            {"invalid_yson", "malformed_probe"},
            {"run", "corrupt_probe"},
            {"lane", "corrupt_probe"},
            {"bucket", "corrupt_probe"},
            {"data", "corrupt_probe"},
            {"empty_range", "invalid_expected_range"},
            {"reversed_range", "invalid_expected_range"},
            {"oversized_range", "validation_state_limit"},
            {"oversized_merged_range", "validation_state_limit"},
         }) {
        SCOPED_TRACE(mutation);
        TTestStateEnvironment stateEnv(LaneKeySchema());
        auto [harness, status] = MakeCombinedHarness(stateEnv);
        std::vector<TInputMessageConstPtr> messages;
        if (mutation == "empty_range") {
            messages = {LaneExpected(7, 7)};
        } else if (mutation == "reversed_range") {
            messages = {LaneExpected(8, 7)};
        } else if (mutation == "oversized_range") {
            messages = {LaneExpected(7, 20'008)};
        } else if (mutation == "oversized_merged_range") {
            messages = {LaneExpected(7, 20'007), LaneExpected(20'007, 20'008)};
        } else {
            auto envelope = ConvertTo<TDeliveryProbeEnvelopePtr>(TYsonString(GetColumnValue<std::string>(LaneObserved(7), "data")));
            if (mutation == "run") {
                envelope->RunId = "other-run";
                envelope->Data = Format("%v/%v/%v", envelope->RunId, envelope->ProducerId, envelope->Sequence);
            } else if (mutation == "lane") {
                envelope->Lane = "other-lane";
            } else if (mutation == "bucket") {
                envelope->BucketStart = 110;
            } else if (mutation == "data") {
                envelope->Data = "corrupted";
            }
            messages = {MakeTestMessage("observed", LaneKey(), RawObservedSchema(), [&] (TMessageBuilder& builder) {
                builder.Payload().Set(std::string("run"), "run_id");
                builder.Payload().Set(std::string("lane"), "lane");
                if (mutation != "null") {
                    builder.Payload().Set(mutation == "invalid_yson" ? std::string("[") : std::string(ConvertToYsonString(envelope).AsStringBuf()), "data");
                }
            })};
        }
        harness->RunEpoch(std::move(messages));
        auto state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
        ASSERT_TRUE(state);
        EXPECT_EQ(state->FirstErrorKind, errorKind);
        EXPECT_LE(state->FirstErrorDetail.size(), 1024u);
        EXPECT_EQ(state->CheckedProbeCount, 0u);
        EXPECT_EQ(state->Producers.size(), mutation == "oversized_merged_range" ? 1u : 0u);
        for (const auto& producer : state->Producers) {
            EXPECT_TRUE(producer->Windows.empty());
        }
        EXPECT_TRUE(status->GetStatus().Errors.empty());
        harness->RunEpoch({}, {LaneTimer("status")});
        EXPECT_EQ(status->GetStatus().Errors.size(), 1u);
        EXPECT_NE(ToString(status->GetStatus().Errors).find(errorKind), std::string::npos);
        EXPECT_EQ(harness->GetTimers().size(), 1u);
        state = stateEnv.ReadKeyState<TIntrusivePtr<TDeliveryValidationState>>("validation", LaneKey());
        EXPECT_EQ(state->FirstErrorKind, errorKind);
    }
}

TEST(TDeliveryProbeOutputReaderTest, PreservesNullableRawValuesAndSourceEventTimestamp)
{
    TTestStateEnvironment stateEnv;
    auto parameters = New<TDeliveryProbeOutputReaderParameters>();
    parameters->RunId = "run";
    parameters->Lane = "lane";
    stateEnv.SetStaticParameters(parameters);
    auto context = TTestRuntimeContextBuilder().RegisterStream("observed", RawObservedSchema()).Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TDeliveryProbeOutputReader>(), context);
    auto sourceSchema = ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf("[{name=data;type=string};{name=unparsed;type=string}]")));
    auto message = MakeTestMessage("source", MakeKey(ui64(0)), sourceSchema, [&] (TMessageBuilder& builder) {
        builder.SetEventTimestamp(TSystemTimestamp(123));
        builder.Payload().Set(std::string("unframed"), "unparsed");
    });
    harness.RunEpoch({message});
    ASSERT_EQ(harness.GetMessages().size(), 1u);
    const auto& observed = harness.GetMessages()[0].Message;
    EXPECT_EQ(observed.EventTimestamp, TSystemTimestamp(123));
    EXPECT_EQ(GetColumn(observed, "data").Type, EValueType::Null);
    EXPECT_EQ(GetColumnValue<std::string>(observed, "unparsed"), "unframed");
    EXPECT_EQ(GetColumnValue<std::string>(observed, "run_id"), "run");
    EXPECT_EQ(GetColumnValue<std::string>(observed, "lane"), "lane");
    EXPECT_EQ(harness.GetMessages()[0].ParentIds, std::vector<TMessageId>{message->MessageId});
}

TEST(TDeliveryProbeGeneratorTest, UsesConfiguredStreamsAndExplicitProbeTimestamp)
{
    TTestStateEnvironment stateEnv(LaneKeySchema());
    auto parameters = New<TDeliveryProbeGeneratorParameters>();
    parameters->RunId = "run";
    parameters->Lane = "lane";
    parameters->WindowSize = TDuration::Seconds(10);
    parameters->GeneratedStreamId = "lane_generated";
    parameters->ExpectedStreamId = "lane_expected";
    stateEnv.SetStaticParameters(parameters);
    auto context = TTestRuntimeContextBuilder()
        .SetKeySchema(LaneKeySchema())
        .SetCurrentTimestamp(TSystemTimestamp(125))
        .RegisterStream("lane_generated", ConvertTo<TTableSchemaPtr>(TYsonString(TStringBuf("[{name=key;type=string};{name=data;type=string}]"))))
        .RegisterStream<TExpectedProbeRange>("lane_expected")
        .Build();
    TProcessFunctionTestHarness harness(stateEnv, New<TDeliveryProbeGenerator>(), context);

    harness.RunEpoch({MakeTestMessage("random", LaneKey(), New<TTableSchema>())});
    ASSERT_EQ(std::ssize(harness.GetMessages()), 2);
    EXPECT_EQ(harness.GetMessages()[0].Message.StreamId, TStreamId("lane_generated"));
    EXPECT_EQ(harness.GetMessages()[1].Message.StreamId, TStreamId("lane_expected"));
    EXPECT_EQ(harness.GetMessages()[0].Message.EventTimestamp, TSystemTimestamp(125));
    EXPECT_EQ(harness.GetMessages()[1].Message.EventTimestamp, TSystemTimestamp(125));
    EXPECT_EQ(GetColumnValue<ui64>(harness.GetMessages()[1].Message, "bucket_start"), 120u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NYdbTest
