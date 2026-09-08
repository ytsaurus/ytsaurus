#include <yt/yt/flow/library/cpp/computation/computation_base.h>
#include <yt/yt/flow/library/cpp/computation/lineage_accumulator.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TInputMessageConstPtr MakeParent(const TStreamId& streamId, int index)
{
    auto schema = New<NTableClient::TTableSchema>();
    TMessageBuilder builder(streamId, schema);
    builder.SetMessageId(TMessageId(Format("parent-%v", index)));
    builder.SetSystemTimestamp(TSystemTimestamp(100));
    builder.SetAlignmentTimestamp(TSystemTimestamp(100));
    builder.SetEventTimestamp(TSystemTimestamp(100));
    return New<TInputMessage>(builder.Finish(), MakeKey(index));
}

std::vector<TMessage> MakeOutputs(const TStreamId& streamId, int count)
{
    std::vector<TMessage> result(count);
    for (auto& message : result) {
        message.StreamId = streamId;
    }
    return result;
}

class TIdentityMetaSetter
    : public IMetaSetter
{
public:
    TFillResult Fill(
        TMessage& /*message*/,
        const TMessageParentsConstPtr& parents,
        const TOutputMessageIdSuffix& /*messageIdSuffix*/) override
    {
        return {.ActualParentMessageIds = parents};
    }

    TFillResult Fill(TTimer& /*timer*/, const TMessageParentsConstPtr& parents) override
    {
        return {.ActualParentMessageIds = parents};
    }
};

TInputTimerConstPtr MakeParentTimer(const TStreamId& streamId)
{
    TTimer timer;
    timer.StreamId = streamId;
    timer.MessageId = TMessageId("parent-timer");
    timer.SystemTimestamp = TSystemTimestamp(100);
    timer.AlignmentTimestamp = TSystemTimestamp(100);
    timer.EventTimestamp = TSystemTimestamp(100);
    timer.TriggerTimestamp = TSystemTimestamp(100);
    timer.Key = MakeKey();
    timer.KeySchema = New<NTableClient::TTableSchema>();
    return New<TInputTimer>(std::move(timer));
}

TInputVisitConstPtr MakeParentVisit(const TStreamId& streamId)
{
    TVisit visit;
    visit.StreamId = streamId;
    visit.MessageId = TMessageId("parent-visit");
    visit.SystemTimestamp = TSystemTimestamp(100);
    visit.AlignmentTimestamp = TSystemTimestamp(100);
    visit.EventTimestamp = TSystemTimestamp(100);
    visit.Key = MakeKey(0);
    return New<TInputVisit>(std::move(visit));
}

const TLineageDeltaValue& GetDeltaValue(
    const TLineageDelta& delta,
    const TStreamId& outputStreamId,
    const TStreamId& parentStreamId)
{
    return GetOrCrash(GetOrCrash(delta, outputStreamId), parentStreamId);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TLineageAccumulatorTest, CountsInputsOncePerDeclaredEdgeIncludingZeroOutput)
{
    const TStreamId input("input");
    const TStreamId timer("timer");
    const TStreamId visit("visit");
    const TStreamId output("output");
    const TStreamId filtered("filtered");
    auto spec = New<TComputationSpec>();
    spec->StreamsDependency[output] = {input, timer, visit};
    spec->StreamsDependency[filtered] = {input};
    const std::vector<TInputMessageConstPtr> messages{MakeParent(input, 0), MakeParent(input, 1)};
    const std::vector<TInputTimerConstPtr> timers{MakeParentTimer(timer)};
    const std::vector<TInputVisitConstPtr> visits{MakeParentVisit(visit)};
    const auto parents = New<TMessageParents>(messages, timers, visits);
    TLineageAccumulator accumulator;
    for (const auto& message : MakeOutputs(output, 100)) {
        accumulator.Add(message, parents);
    }
    auto delta = accumulator.Finish();
    AddLineageInputs(&delta, spec, messages, timers, visits);

    const auto& messageDelta = GetDeltaValue(delta, output, input);
    EXPECT_DOUBLE_EQ(messageDelta.Count, 50);
    EXPECT_DOUBLE_EQ(messageDelta.InputCount, 2);
    EXPECT_DOUBLE_EQ(messageDelta.InputByteSize, messages[0]->ByteSize + messages[1]->ByteSize);
    const auto& timerDelta = GetDeltaValue(delta, output, timer);
    EXPECT_DOUBLE_EQ(timerDelta.Count, 25);
    EXPECT_DOUBLE_EQ(timerDelta.InputCount, 1);
    EXPECT_DOUBLE_EQ(timerDelta.InputByteSize, timers[0]->ByteSize);
    const auto& visitDelta = GetDeltaValue(delta, output, visit);
    EXPECT_DOUBLE_EQ(visitDelta.Count, 25);
    EXPECT_DOUBLE_EQ(visitDelta.InputCount, 1);
    EXPECT_DOUBLE_EQ(visitDelta.InputByteSize, visits[0]->ByteSize);
    EXPECT_EQ(delta.at(filtered).size(), 1u);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, filtered, input).Count, 0);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, filtered, input).InputCount, 2);

    AddLineageInput(&delta, spec, input, 3, 100);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, output, input).InputCount, 5);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, filtered, input).InputCount, 5);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, output, input).Count, 50);
}

TEST(TLineageAccumulatorTest, AttributesBatchOutputsUniformlyAcrossParents)
{
    const TStreamId firstInput("first_input");
    const TStreamId secondInput("second_input");
    const TStreamId output("output");

    std::vector<TInputMessageConstPtr> parentMessages;
    for (int index = 0; index < 80; ++index) {
        parentMessages.push_back(MakeParent(firstInput, index));
    }
    for (int index = 80; index < 100; ++index) {
        parentMessages.push_back(MakeParent(secondInput, index));
    }
    auto parents = New<TMessageParents>(
        std::move(parentMessages),
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto outputs = MakeOutputs(output, 250);
    double outputByteSize = 0;
    TLineageAccumulator accumulator;
    for (const auto& outputMessage : outputs) {
        outputByteSize += GetMessageByteSize(outputMessage);
        accumulator.Add(outputMessage, parents);
    }
    const auto delta = accumulator.Finish();

    const auto& parentDeltas = GetOrCrash(delta, output);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, firstInput).Count, 200);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, firstInput).ByteSize, outputByteSize * 0.8);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, secondInput).Count, 50);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, secondInput).ByteSize, outputByteSize * 0.2);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, firstInput).Count / 80, 2.5);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, secondInput).Count / 20, 2.5);
}

TEST(TLineageAccumulatorTest, AttributesEachOutputStreamIndependently)
{
    const TStreamId input("input");
    const TStreamId firstOutput("first_output");
    const TStreamId secondOutput("second_output");

    std::vector<TInputMessageConstPtr> parentMessages;
    for (int index = 0; index < 10; ++index) {
        parentMessages.push_back(MakeParent(input, index));
    }
    auto parents = New<TMessageParents>(
        std::move(parentMessages),
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto outputs = MakeOutputs(firstOutput, 30);
    auto secondOutputs = MakeOutputs(secondOutput, 20);
    const double firstOutputByteSize = GetMessageByteSize(outputs.front()) * outputs.size();
    const double secondOutputByteSize = GetMessageByteSize(secondOutputs.front()) * secondOutputs.size();
    outputs.insert(
        outputs.end(),
        std::make_move_iterator(secondOutputs.begin()),
        std::make_move_iterator(secondOutputs.end()));
    TLineageAccumulator accumulator;
    for (const auto& outputMessage : outputs) {
        accumulator.Add(outputMessage, parents);
    }
    const auto delta = accumulator.Finish();

    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, firstOutput, input).Count, 30);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, firstOutput, input).ByteSize, firstOutputByteSize);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, secondOutput, input).Count, 20);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, secondOutput, input).ByteSize, secondOutputByteSize);
}

TEST(TLineageAccumulatorTest, AggregatesDistinctSingleParentSetsByStreamEdge)
{
    const TStreamId input("input");
    const TStreamId output("output");

    TLineageAccumulator accumulator;
    double outputByteSize = 0;
    for (int index = 0; index < 1000; ++index) {
        auto parents = New<TMessageParents>(
            std::vector<TInputMessageConstPtr>{MakeParent(input, index)},
            std::vector<TInputTimerConstPtr>{},
            std::vector<TInputVisitConstPtr>{});
        TMessage outputMessage;
        outputMessage.StreamId = output;
        outputByteSize += GetMessageByteSize(outputMessage);
        accumulator.Add(outputMessage, parents);
    }

    const auto delta = accumulator.Finish();

    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, output, input).Count, 1000);
    EXPECT_DOUBLE_EQ(GetDeltaValue(delta, output, input).ByteSize, outputByteSize);
}

TEST(TLineageAccumulatorTest, AttributesOutputTimersAcrossAllParentKinds)
{
    const TStreamId messageInput("message_input");
    const TStreamId timerInput("timer_input");
    const TStreamId visitInput("visit_input");
    const TStreamId outputTimerStream("output_timer");

    TMessageParentsConstPtr parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{MakeParent(messageInput, 0)},
        std::vector<TInputTimerConstPtr>{MakeParentTimer(timerInput)},
        std::vector<TInputVisitConstPtr>{MakeParentVisit(visitInput)});
    TTimer outputTimer;
    outputTimer.StreamId = outputTimerStream;
    const double outputTimerByteSize = GetTimerByteSize(outputTimer);

    TLineageAccumulator accumulator;
    accumulator.Add(outputTimer, parents);
    const auto delta = accumulator.Finish();

    const auto& parentDeltas = GetOrCrash(delta, outputTimerStream);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, messageInput).Count, 1.0 / 3);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, messageInput).ByteSize, outputTimerByteSize / 3);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, timerInput).Count, 1.0 / 3);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, timerInput).ByteSize, outputTimerByteSize / 3);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, visitInput).Count, 1.0 / 3);
    EXPECT_DOUBLE_EQ(GetOrCrash(parentDeltas, visitInput).ByteSize, outputTimerByteSize / 3);
}

TEST(TLineageAccumulatorTest, CollectorCountsOnlyDistributedSourceOutputs)
{
    const TStreamId input("input");
    const TStreamId output("output");
    auto parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{MakeParent(input, 0)},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});
    auto collector = New<TRootOutputCollector>(
        New<TComputationSpec>(),
        New<TIdentityMetaSetter>(),
        /*supportsDistribute*/ true);

    TMessage dropped;
    dropped.StreamId = output;
    collector->AddMessage(
        std::move(dropped),
        parents,
        TOutputMessageIdSuffix::FromSequenceNumber(),
        /*distribute*/ false);
    TMessage distributed;
    distributed.StreamId = output;
    const double distributedByteSize = GetMessageByteSize(distributed);
    collector->AddMessage(
        std::move(distributed),
        parents,
        TOutputMessageIdSuffix::FromSequenceNumber(),
        /*distribute*/ true);

    const auto result = collector->CollectResult();

    ASSERT_EQ(result.OutputMessagesDistribute, (std::vector<bool>{false, true}));
    EXPECT_DOUBLE_EQ(GetDeltaValue(result.LineageDelta, output, input).Count, 1);
    EXPECT_DOUBLE_EQ(GetDeltaValue(result.LineageDelta, output, input).ByteSize, distributedByteSize);
}

TEST(TLineageAccumulatorTest, CollectorCanDeferSourceLineageUntilPublication)
{
    const TStreamId input("input");
    const TStreamId output("output");
    auto parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{MakeParent(input, 0)},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});
    auto collector = New<TRootOutputCollector>(
        New<TComputationSpec>(),
        New<TIdentityMetaSetter>(),
        /*supportsDistribute*/ true,
        /*collectLineage*/ false);

    TMessage distributed;
    distributed.StreamId = output;
    collector->AddMessage(
        std::move(distributed),
        parents,
        TOutputMessageIdSuffix::FromSequenceNumber(),
        /*distribute*/ true);

    const auto result = collector->CollectResult();

    EXPECT_TRUE(result.LineageDelta.empty());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
