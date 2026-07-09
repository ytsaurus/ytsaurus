#include "timer.h"

#include "payload_validation.h"

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NFlow {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

// Important for performance, because containers copy elements instead of moving if move-constructor is not noexcept.
static_assert(std::is_nothrow_move_constructible<TTimer>::value, "TTimer copy constructor must be noexcept");

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TTimer& timer, TStringBuf /*spec*/)
{
    builder->AppendFormat("{MessageId: %v, SystemTimestamp: %v, EventTimestamp: %v, StreamId: %v, TriggerTimestamp: %v, Key: %v, KeySchema: %v}",
        timer.MessageId,
        timer.SystemTimestamp,
        timer.EventTimestamp,
        timer.StreamId,
        timer.TriggerTimestamp,
        timer.Key,
        timer.KeySchema);
}

i64 GetTimerMetaByteSize(const TTimerMeta& meta)
{
    return GetMessageMetaByteSize(meta);
}

i64 GetTimerByteSize(const TTimer& timer)
{
    return GetTimerMetaByteSize(timer) - sizeof(TTimerMeta) + sizeof(timer) + timer.Key.Underlying().GetSpaceUsed();
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TTimer& timer, int columnId)
{
    return timer.Key.Underlying()[columnId];
}

NTableClient::TUnversionedValue GetColumn(const TTimer& timer, TStringBuf columnName)
{
    YT_ASSERT(timer.KeySchema);
    auto columnId = timer.KeySchema->GetColumnIndexOrThrow(columnName);
    return GetColumn(timer, columnId);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TError DoValidateTimer(const TTimer& timer, const TValidatePayloadOptions& options)
{
    if (timer.MessageId.Underlying().empty()) {
        return TError("timer.MessageId is empty");
    }
    if (timer.SystemTimestamp == ZeroSystemTimestamp) {
        return TError("timer.SystemTimestamp is undefined");
    }
    if (timer.SystemTimestamp == InfinitySystemTimestamp) {
        return TError("timer.SystemTimestamp is infinity");
    }
    if (timer.EventTimestamp == ZeroSystemTimestamp) {
        return TError("timer.EventTimestamp is undefined");
    }
    if (timer.EventTimestamp == InfinitySystemTimestamp) {
        return TError("timer.EventTimestamp is infinity");
    }
    if (timer.TriggerTimestamp == ZeroSystemTimestamp) {
        return TError("timer.TriggerTimestamp is undefined");
    }
    if (timer.TriggerTimestamp == InfinitySystemTimestamp) {
        return TError("timer.TriggerTimestamp is infinity");
    }
    if (options.ValidateValues) {
        if (GetTimerByteSize(timer) > NTableClient::MaxStringValueLength) {
            return TError("Timer is too big: size %v, limit %v",
                GetTimerByteSize(timer),
                NTableClient::MaxStringValueLength);
        }
    }
    return DoValidatePayload("timer.Key", timer.Key, "timer.KeySchema", timer.KeySchema, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ValidateTimer(const TTimer& timer, const TValidatePayloadOptions& options)
{
    auto error = DoValidateTimer(timer, options);
    if (!error.IsOK()) {
        THROW_ERROR error
            << TErrorAttribute("message_id", timer.MessageId.Underlying())
            << TErrorAttribute("stream_id", timer.StreamId);
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(NProto::TTimer* protoTimer, const TTimer& timer)
{
    // TimerMeta fields.
    protoTimer->set_message_id(ToProto<TProtobufString>(timer.MessageId));
    protoTimer->set_system_timestamp(ToProto(timer.SystemTimestamp));
    protoTimer->set_event_timestamp(ToProto(timer.EventTimestamp));
    protoTimer->set_stream_id(ToProto<TProtobufString>(timer.StreamId));
    // Timer fields.
    protoTimer->set_trigger_timestamp(ToProto(timer.TriggerTimestamp));
    protoTimer->set_key(ToProto<TProtobufString>(timer.Key));
}

void FromProto(TTimer* timer, const NProto::TTimer& protoTimer)
{
    // TimerMeta fields.
    timer->MessageId = TMessageId(protoTimer.message_id());
    timer->SystemTimestamp = FromProto<TSystemTimestamp>(protoTimer.system_timestamp());
    timer->EventTimestamp = FromProto<TSystemTimestamp>(protoTimer.event_timestamp());
    timer->StreamId = FromProto<TStreamId>(protoTimer.stream_id());
    // Timer fields.
    timer->TriggerTimestamp = FromProto<TSystemTimestamp>(protoTimer.trigger_timestamp());
    timer->Key = FromProto<TKey>(protoTimer.key());
}

////////////////////////////////////////////////////////////////////////////////

const TMessageMeta& TInputTimer::GetMeta() const
{
    return *this;
}

TInputTimer::TInputTimer(TTimer&& timer, const NTableClient::TTableSchemaPtr& expectedKeySchema)
    : TTimer(std::move(timer))
    , ByteSize(GetTimerByteSize(*this))
{
    try {
        ValidateTimer(*this, {.ExpectedSchema = expectedKeySchema});
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Failed to create timer")
            << TError(ex);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInputTimerConstPtr& timer, TStringBuf spec)
{
    FormatValue(builder, static_cast<const TTimer&>(*timer), spec);
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TInputTimerConstPtr& timer, int columnId)
{
    return GetColumn(*timer, columnId);
}

NTableClient::TUnversionedValue GetColumn(const TInputTimerConstPtr& timer, TStringBuf columnName)
{
    return GetColumn(*timer, columnName);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
