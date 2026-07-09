#include <yt/yt/flow/library/cpp/computation/event_timestamp_assigner.h>
#include <yt/yt/flow/library/cpp/computation/meta_setter.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TSwiftMergeMetaSetterTest
    : public ::testing::Test
{
protected:
    const TStreamId InputStreamId = TStreamId("in");
    const TStreamId OutputStreamId = TStreamId("out");
    const TUniqueSeqNo SeqNo = TUniqueSeqNo(42);

    TComputationSpecPtr MakeSpec()
    {
        auto spec = New<TComputationSpec>();
        spec->InputStreamIds = {InputStreamId};
        spec->OutputStreamIds = {OutputStreamId};
        spec->StreamsDependency = {
            {OutputStreamId, {InputStreamId}},
        };
        return spec;
    }

    TInputMessageConstPtr MakeParent(
        const std::string& messageId,
        ui64 systemTimestamp,
        ui64 eventTimestamp,
        ui64 alignmentTimestamp)
    {
        auto schema = New<NTableClient::TTableSchema>();
        TMessageBuilder builder(InputStreamId, schema);
        builder.SetMessageId(TMessageId(messageId));
        builder.SetSystemTimestamp(TSystemTimestamp(systemTimestamp));
        builder.SetEventTimestamp(TSystemTimestamp(eventTimestamp));
        builder.SetAlignmentTimestamp(TSystemTimestamp(alignmentTimestamp));
        return New<TInputMessage>(builder.Finish(), MakeKey(messageId));
    }

    TMessage MakeTrivialOutputMessage()
    {
        TMessage msg;
        msg.StreamId = OutputStreamId;
        return msg;
    }

    IMetaSetterPtr MakeSetter(TComputationSpecPtr spec = nullptr)
    {
        return CreateSwiftMergeMetaSetter(
            spec ? std::move(spec) : MakeSpec(),
            SeqNo,
            CreateEventTimestampAssigner(/*spec=*/nullptr));
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TSwiftMergeMetaSetterTest, SingleParentInheritsIdAndTimestamps)
{
    auto setter = MakeSetter();
    auto parent = MakeParent("p1", /*systemTimestamp=*/100, /*eventTimestamp=*/90, /*alignmentTimestamp=*/95);
    auto parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{parent},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto out = MakeTrivialOutputMessage();
    setter->Fill(out, parents);

    // Inherited ID is "<parent.MessageId>-<stream>:<offset>".
    EXPECT_TRUE(out.MessageId.Underlying().starts_with(std::string(parent->MessageId.Underlying()) + "-out:"))
        << "MessageId=" << out.MessageId.Underlying();
    EXPECT_EQ(out.SystemTimestamp, parent->SystemTimestamp);
    EXPECT_EQ(out.AlignmentTimestamp, parent->AlignmentTimestamp);
    EXPECT_EQ(out.EventTimestamp, parent->EventTimestamp);
}

TEST_F(TSwiftMergeMetaSetterTest, SingleParentDistinctOutputIndices)
{
    auto setter = MakeSetter();
    auto parent = MakeParent("p1", 100, 90, 95);
    auto parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{parent},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto out1 = MakeTrivialOutputMessage();
    setter->Fill(out1, parents);
    auto out2 = MakeTrivialOutputMessage();
    setter->Fill(out2, parents);

    EXPECT_NE(out1.MessageId, out2.MessageId);
}

TEST_F(TSwiftMergeMetaSetterTest, MultiParentUsesOrderedIdAndMergedTimestamps)
{
    auto setter = MakeSetter();
    auto p1 = MakeParent("p1", /*systemTimestamp=*/100, /*eventTimestamp=*/80, /*alignmentTimestamp=*/95);
    auto p2 = MakeParent("p2", /*systemTimestamp=*/150, /*eventTimestamp=*/90, /*alignmentTimestamp=*/110);
    auto p3 = MakeParent("p3", /*systemTimestamp=*/120, /*eventTimestamp=*/70, /*alignmentTimestamp=*/100);
    auto parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{p1, p2, p3},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto out = MakeTrivialOutputMessage();
    setter->Fill(out, parents);

    // SystemTimestamp = max over parents; AlignmentTimestamp / EventTimestamp = min over parents.
    EXPECT_EQ(out.SystemTimestamp, TSystemTimestamp(150));
    EXPECT_EQ(out.AlignmentTimestamp, TSystemTimestamp(95));
    EXPECT_EQ(out.EventTimestamp, TSystemTimestamp(70));

    // Ordered ID format: "<16hex>-<stream>:<offset>".
    EXPECT_TRUE(out.MessageId.Underlying().starts_with("000000000000002a-out:"))
        << "MessageId=" << out.MessageId.Underlying();
}

TEST_F(TSwiftMergeMetaSetterTest, MultiParentDistinctOutputIndices)
{
    auto setter = MakeSetter();
    auto p1 = MakeParent("p1", 100, 80, 95);
    auto p2 = MakeParent("p2", 150, 90, 110);
    auto parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{p1, p2},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto out1 = MakeTrivialOutputMessage();
    setter->Fill(out1, parents);
    auto out2 = MakeTrivialOutputMessage();
    setter->Fill(out2, parents);

    EXPECT_NE(out1.MessageId, out2.MessageId);
}

TEST_F(TSwiftMergeMetaSetterTest, MixedSingleAndMultiParentsShareTheSetter)
{
    auto setter = MakeSetter();
    auto p1 = MakeParent("p1", 100, 80, 95);
    auto p2 = MakeParent("p2", 150, 90, 110);

    auto singleParents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{p1},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});
    auto mergedParents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{p1, p2},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto outSingle = MakeTrivialOutputMessage();
    setter->Fill(outSingle, singleParents);

    auto outMerged = MakeTrivialOutputMessage();
    setter->Fill(outMerged, mergedParents);

    EXPECT_TRUE(outSingle.MessageId.Underlying().starts_with(std::string(p1->MessageId.Underlying()) + "-out:"));
    EXPECT_TRUE(outMerged.MessageId.Underlying().starts_with("000000000000002a-out:"));
    EXPECT_EQ(outSingle.SystemTimestamp, p1->SystemTimestamp);
    EXPECT_EQ(outMerged.SystemTimestamp, p2->SystemTimestamp); // max(100, 150).
}

TEST_F(TSwiftMergeMetaSetterTest, EmptyParentsThrow)
{
    auto setter = MakeSetter();
    auto parents = New<TMessageParents>(
        std::vector<TInputMessageConstPtr>{},
        std::vector<TInputTimerConstPtr>{},
        std::vector<TInputVisitConstPtr>{});

    auto out = MakeTrivialOutputMessage();
    EXPECT_THROW(setter->Fill(out, parents), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
