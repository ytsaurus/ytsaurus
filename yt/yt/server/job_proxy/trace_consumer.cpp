#include "trace_consumer.h"

#include <yt/yt/library/formats/format.h>

namespace NYT::NJobProxy {

using namespace NFormats;

////////////////////////////////////////////////////////////////////////////////

static const TString TimestampAttributeKey = "ts";
static const TString ThreadIdAttributeKey = "tid";

static const THashSet<TString> TraceAttributeKeyWhitelist = {
    TimestampAttributeKey,
    ThreadIdAttributeKey,
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

            double timestamp = AttributeConsumer_.GetAttributes()->Get<double>(TimestampAttributeKey);
            int threadId = AttributeConsumer_.GetAttributes()->Get<int>(ThreadIdAttributeKey);

            AttributeConsumer_.GetAttributes()->Clear();

            HasTrace_ = true;

            JobTraceEventProcessor_->OnEvent(TTraceEvent{
                .RawEvent = std::move(event),
                .ThreadId = threadId,
                .Timestamp = timestamp,
            });
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy

