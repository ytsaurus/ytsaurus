#include <yt/yt/flow/library/cpp/common/output_collector.h>

#include <yt/yt/flow/library/cpp/common/key.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TLegacyOutputCollector
    : public IOutputCollector
{
public:
    IOutputCollectorPtr SetParents(
        const std::vector<TInputMessageConstPtr>& /*messages*/,
        const std::vector<TInputTimerConstPtr>& /*timers*/,
        const std::vector<TInputVisitConstPtr>& /*visits*/) override
    {
        return nullptr;
    }

    void AddTimer(TSystemTimestamp /*triggerTimestamp*/, std::optional<TSystemTimestamp> /*eventTimestamp*/) override
    { }

    void AddTimer(const TStreamId& /*streamId*/, TSystemTimestamp /*triggerTimestamp*/, std::optional<TSystemTimestamp> /*eventTimestamp*/) override
    { }

    void AddTimer(TTimer&& /*timer*/) override
    { }

private:
    void DoAddMessage(TMessage&& /*message*/, TAddMessageOptions /*options*/) override
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TOutputCollectorTest, FailedMessageCaptureIsOptional)
{
    auto output = New<TLegacyOutputCollector>();
    TMessageBuilder builder(TStreamId("input"), New<NTableClient::TTableSchema>());
    builder.SetMessageId(TMessageId("message"));
    builder.SetSystemTimestamp(TSystemTimestamp(1));
    builder.SetAlignmentTimestamp(TSystemTimestamp(1));
    builder.SetEventTimestamp(TSystemTimestamp(1));
    auto message = New<TInputMessage>(builder.Finish(), MakeKey(1));

    EXPECT_THROW(output->AddFailedMessage(message, TError("Invalid input")), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
