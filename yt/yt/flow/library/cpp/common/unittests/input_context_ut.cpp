#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/library/cpp/common/input_context.h>
#include <yt/yt/flow/library/cpp/common/key.h>
#include <yt/yt/flow/library/cpp/common/message.h>
#include <yt/yt/flow/library/cpp/common/timer.h>
#include <yt/yt/flow/library/cpp/common/visit.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NFlow {
namespace {

using namespace NTableClient;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

const auto PayloadSchema = ConvertTo<TTableSchemaPtr>(TYsonStringBuf(
    R"""([{name="word"; type="string";};])"""));

constexpr auto ValidTs = TSystemTimestamp(1'500'000'000);

TInputMessageConstPtr MakeMessage(const std::string& word)
{
    TMessageBuilder builder(TStreamId("messages"), PayloadSchema);
    builder.Payload().SetValue(MakeUnversionedStringValue(word, 0));
    builder.SetMessageId(TMessageId(word));
    builder.SetSystemTimestamp(ValidTs);
    builder.SetAlignmentTimestamp(ValidTs);
    builder.SetEventTimestamp(ValidTs);
    return New<TInputMessage>(builder.Finish(), MakeKey(word));
}

TInputTimerConstPtr MakeTimer(const std::string& word)
{
    TTimer timer;
    timer.MessageId = TMessageId("timer-" + word);
    timer.StreamId = TStreamId("timers");
    timer.SystemTimestamp = ValidTs;
    timer.AlignmentTimestamp = ValidTs;
    timer.EventTimestamp = ValidTs;
    timer.TriggerTimestamp = ValidTs;
    timer.Key = MakeKey(word);
    timer.KeySchema = PayloadSchema;
    return New<TInputTimer>(std::move(timer), PayloadSchema);
}

TInputVisitConstPtr MakeVisit(const std::string& word)
{
    TVisit visit;
    visit.MessageId = TMessageId("visit-" + word);
    visit.StreamId = TStreamId("visits");
    visit.SystemTimestamp = ValidTs;
    visit.AlignmentTimestamp = ValidTs;
    visit.EventTimestamp = ValidTs;
    visit.Key = MakeKey(word);
    return New<TInputVisit>(std::move(visit));
}

IInputContextPtr MakeContext()
{
    return New<TInputContext>(
        std::vector<TInputMessageConstPtr>{MakeMessage("message")},
        std::vector<TInputTimerConstPtr>{MakeTimer("timer")},
        std::vector<TInputVisitConstPtr>{MakeVisit("visit")});
}

////////////////////////////////////////////////////////////////////////////////

TEST(TExtractKeysOptionsTest, DefaultOptionsTakeEveryKind)
{
    auto keys = ExtractKeys(MakeContext(), TExtractKeysOptions{});
    EXPECT_EQ(keys, (THashSet<TKey>{MakeKey("message"), MakeKey("timer"), MakeKey("visit")}));
}

TEST(TExtractKeysOptionsTest, VisitsCanBeLeftOut)
{
    auto keys = ExtractKeys(MakeContext(), TExtractKeysOptions{.Visits = false});
    EXPECT_EQ(keys, (THashSet<TKey>{MakeKey("message"), MakeKey("timer")}));
}

TEST(TExtractKeysOptionsTest, VisitsOnly)
{
    auto keys = ExtractKeys(MakeContext(), TExtractKeysOptions{.Messages = false, .Timers = false});
    EXPECT_EQ(keys, (THashSet<TKey>{MakeKey("visit")}));
}

TEST(TExtractKeysOptionsTest, DefaultArgumentTakesEveryKind)
{
    auto context = MakeContext();
    EXPECT_EQ(ExtractKeys(context), ExtractKeys(context, TExtractKeysOptions{}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
