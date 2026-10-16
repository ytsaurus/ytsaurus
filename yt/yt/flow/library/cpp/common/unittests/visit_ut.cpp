#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TVisitTest, ProtoRoundTrip)
{
    TVisit visit;
    visit.MessageId = TMessageId("m-1");
    visit.SystemTimestamp = TSystemTimestamp(1'000);
    visit.EventTimestamp = TSystemTimestamp(1'001);
    visit.StreamId = TStreamId("visit-stream");
    visit.Key = MakeUintKey(42);

    NProto::TVisit proto;
    ToProto(&proto, visit);
    TVisit restored;
    FromProto(&restored, proto);

    EXPECT_EQ(restored.MessageId, visit.MessageId);
    EXPECT_EQ(restored.SystemTimestamp, visit.SystemTimestamp);
    EXPECT_EQ(restored.EventTimestamp, visit.EventTimestamp);
    EXPECT_EQ(restored.StreamId, visit.StreamId);
    EXPECT_EQ(restored.Key, visit.Key);
}

TEST(TVisitTest, ValidateRejectsEmptyKey)
{
    TVisit visit;
    visit.MessageId = TMessageId("m-1");
    visit.SystemTimestamp = TSystemTimestamp(1);
    visit.EventTimestamp = TSystemTimestamp(1);
    visit.AlignmentTimestamp = TSystemTimestamp(1);
    visit.StreamId = TStreamId("s");
    // Key intentionally left empty.

    EXPECT_THROW(ValidateVisit(visit), std::exception);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
