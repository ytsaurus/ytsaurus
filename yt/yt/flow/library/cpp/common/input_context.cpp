#include "input_context.h"

#include "message.h"
#include "payload.h"
#include "schema.h"
#include "timer.h"
#include "visit.h"

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

TInputContext::TInputContext(
    const std::vector<TInputMessageConstPtr>& messages,
    const std::vector<TInputTimerConstPtr>& timers,
    const std::vector<TInputVisitConstPtr>& visits)
    : Messages_(messages)
    , Timers_(timers)
    , Visits_(visits)
{ }

const std::vector<TInputMessageConstPtr>& TInputContext::GetMessages() const
{
    return Messages_;
}

const std::vector<TInputTimerConstPtr>& TInputContext::GetTimers() const
{
    return Timers_;
}

const std::vector<TInputVisitConstPtr>& TInputContext::GetVisits() const
{
    return Visits_;
}

////////////////////////////////////////////////////////////////////////////////

THashSet<TKey> ExtractKeys(
    const IInputContextPtr& context,
    const NTableClient::TTableSchemaPtr& schemaOverride,
    const std::optional<THashSet<TStreamId>>& streamFilter,
    const IPayloadConverterCachePtr& converterCache)
{
    THashSet<TKey> result;
    auto passes = [&] (const TStreamId& streamId) {
        return !streamFilter || streamFilter->contains(streamId);
    };
    for (const auto& message : context->GetMessages()) {
        if (!passes(message->StreamId)) {
            continue;
        }
        if (schemaOverride) {
            auto converted = ConvertPayloadToNewSchema(
                message->Payload,
                message->PayloadSchema,
                schemaOverride,
                converterCache);
            result.emplace(converted.Underlying());
        } else {
            result.insert(message->Key);
        }
    }
    for (const auto& timer : context->GetTimers()) {
        if (!passes(timer->StreamId)) {
            continue;
        }
        if (schemaOverride) {
            auto converted = ConvertPayloadToNewSchema(
                TPayload(timer->Key.Underlying()),
                timer->KeySchema,
                schemaOverride,
                converterCache);
            result.emplace(converted.Underlying());
        } else {
            result.insert(timer->Key);
        }
    }
    for (const auto& visit : context->GetVisits()) {
        if (!passes(visit->StreamId)) {
            continue;
        }
        // Visits already carry the bucket key — no schema override is supported.
        result.insert(visit->Key);
    }
    return result;
}

THashSet<TKey> ExtractKeys(const IInputContextPtr& context)
{
    THashSet<TKey> result;
    result.reserve(context->GetMessages().size() + context->GetTimers().size() + context->GetVisits().size());
    for (const auto& message : context->GetMessages()) {
        result.insert(message->Key);
    }
    for (const auto& timer : context->GetTimers()) {
        result.insert(timer->Key);
    }
    for (const auto& visit : context->GetVisits()) {
        result.insert(visit->Key);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
