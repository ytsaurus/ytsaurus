#include "trace_consumer.h"

#include <yt/yt/library/formats/format.h>

namespace NYT::NJobProxy {

using namespace NFormats;
using namespace NJobTrackerClient;

////////////////////////////////////////////////////////////////////////////////

static const std::string ControlTraceEventName = "ytControlEvent";

////////////////////////////////////////////////////////////////////////////////

static const TString TimestampAttributeKey = "ts";
static const TString ThreadIdAttributeKey = "tid";
static const TString ProcessIdAttributeKey = "pid";
static const TString EventNameAttributeKey = "name";
static const TString TraceIdAttributeKey = "traceId";
static const TString EventTypeAttributeKey = "type";

static const THashSet<TString> TraceAttributeKeyWhitelist = {
    TimestampAttributeKey,
    ThreadIdAttributeKey,
    ProcessIdAttributeKey,
    EventNameAttributeKey,
    TraceIdAttributeKey,
    EventTypeAttributeKey,
};

////////////////////////////////////////////////////////////////////////////////

TTraceConsumer::TTraceConsumer(TJobTraceEventProcessorPtr jobTraceEventProcessor)
    : JobTraceEventProcessor_(std::move(jobTraceEventProcessor))
    , Attributes_(NYTree::CreateEphemeralAttributes())
    , AttributeConsumer_(Attributes_.Get(), TraceAttributeKeyWhitelist)
    , JsonWriter_(CreateJsonWriter())
{ }

std::unique_ptr<NJson::IJsonWriter> TTraceConsumer::CreateJsonWriter()
{
    return NJson::CreateJsonWriter(&JsonStream_, /*pretty*/ false);
}

void TTraceConsumer::OnMyListItem()
{
    Forward(
        std::vector<IYsonConsumer*>{JsonWriter_.get(), &AttributeConsumer_},
        [this] {
            JsonWriter_->Flush();
            auto event = JsonStream_.Str();
            JsonStream_.Clear();
            JsonWriter_ = CreateJsonWriter();

            auto finallyGuard = Finally([&] {
                AttributeConsumer_.GetAttributes()->Clear();
            });

            auto eventName = AttributeConsumer_.GetAttributes()->Find<std::string>(EventNameAttributeKey);

            if (eventName == ControlTraceEventName) {
                try {
                    auto traceId = TJobTraceId(TGuid::FromString(AttributeConsumer_.GetAttributes()->Get<std::string>(TraceIdAttributeKey)));
                    auto processId = AttributeConsumer_.GetAttributes()->Get<int>(ProcessIdAttributeKey);
                    auto type = AttributeConsumer_.GetAttributes()->Get<EJobTraceEventType>(EventTypeAttributeKey);

                    JobTraceEventProcessor_->OnControlEvent(TTraceControlEvent{
                        .Type = type,
                        .TraceId = traceId,
                        .ProcessId = processId,
                    });
                    return;
                } catch (const std::exception& ex) {
                    THROW_ERROR_EXCEPTION("Failed to process control trace event")
                        << TErrorAttribute("event", event)
                        << ex;
                }
            }

            try {
                auto timestamp = AttributeConsumer_.GetAttributes()->Get<double>(TimestampAttributeKey);
                auto threadId = AttributeConsumer_.GetAttributes()->Get<int>(ThreadIdAttributeKey);
                auto processId = AttributeConsumer_.GetAttributes()->Find<int>(ProcessIdAttributeKey);

                HasTrace_ = true;

                JobTraceEventProcessor_->OnEvent(TTraceEvent{
                    .RawEvent = std::move(event),
                    .ThreadId = threadId,
                    .Timestamp = timestamp,
                    .ProcessId = processId,
                });
            } catch (const std::exception& ex) {
                THROW_ERROR_EXCEPTION("Failed to process job trace event")
                    << TErrorAttribute("event", event)
                    << ex;
            }
        });
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

