#include "flow_queue_meta.h"

#include <yt/yt/flow/library/cpp/common/message.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TFlowQueueMeta::Register(TRegistrar registrar)
{
    registrar.Parameter("event_timestamp", &TThis::EventTimestamp)
        .Default();
    registrar.Parameter("event_timestamp_deltas", &TThis::EventTimestampDeltas)
        .Default();
    registrar.Parameter("event_watermark", &TThis::EventWatermark)
        .Default();
    registrar.Parameter("pure_heartbeat", &TThis::PureHeartbeat)
        .Default(false)
        .DontSerializeDefault();
}

TFlowQueueMeta BuildFlowQueueMeta(const std::vector<TOutputMessageConstPtr>& messages)
{
    if (messages.empty()) {
        return {};
    }

    TFlowQueueMeta meta;
    meta.EventTimestamp = messages[0]->EventTimestamp;
    for (const auto& message : messages) {
        meta.EventTimestamp = std::min(*meta.EventTimestamp, message->EventTimestamp);
    }
    for (const auto& message : messages) {
        meta.EventTimestampDeltas.push_back(message->EventTimestamp.Underlying() - meta.EventTimestamp->Underlying());
    }
    return meta;
}

TFlowQueueMeta BuildFlowQueueMeta(const TMessage& message)
{
    TFlowQueueMeta meta;
    meta.EventTimestamp = message.EventTimestamp;
    return meta;
}

void ApplyFlowQueueMeta(const TFlowQueueMeta& meta, TMessage& message, int index)
{
    if (meta.EventTimestamp) {
        message.EventTimestamp = *meta.EventTimestamp;
        if (index < std::ssize(meta.EventTimestampDeltas)) {
            message.EventTimestamp = TSystemTimestamp(message.EventTimestamp.Underlying() + meta.EventTimestampDeltas[index]);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
