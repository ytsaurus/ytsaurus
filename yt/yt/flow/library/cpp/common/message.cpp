#include "message.h"

#include "key.h"
#include "payload_validation.h"
#include "spec.h"
#include "stream_spec_storage.h"

#include <yt/yt/flow/library/cpp/misc/lexicographically_serialize.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

namespace NYT::NFlow {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

// Important for performance, because containers copy elements instead of moving if move-constructor is not noexcept.
static_assert(std::is_nothrow_move_constructible<TMessage>::value, "TMessage copy constructor must be noexcept");

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TMessage& message, TStringBuf /*spec*/)
{
    builder->AppendFormat("{MessageId: %v, SystemTimestamp: %v, EventTimestamp: %v, StreamId: %v, Payload: %v, PayloadSchema: %v}",
        message.MessageId,
        message.SystemTimestamp,
        message.EventTimestamp,
        message.StreamId,
        message.Payload,
        message.PayloadSchema);
}

i64 GetMessageMetaByteSize(const TMessageMeta& message)
{
    i64 size = 0;
    size += sizeof(message);
    size += message.MessageId.Capacity();
    size += message.StreamId.Capacity();
    return size;
}

i64 GetMessageByteSize(const TMessage& message)
{
    return GetMessageMetaByteSize(message) - sizeof(TMessageMeta) + sizeof(message) + message.Payload.Underlying().GetSpaceUsed();
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NProto::TMessage* protoMessage,
    const TMessage& message,
    const TStreamSpecsPtr& specStorage)
{
    YT_VERIFY(specStorage);

    protoMessage->set_message_id(ToProto<TProtobufString>(message.MessageId));
    protoMessage->set_system_timestamp(ToProto(message.SystemTimestamp));
    protoMessage->set_alignment_timestamp(ToProto(message.AlignmentTimestamp));
    protoMessage->set_event_timestamp(ToProto(message.EventTimestamp));
    protoMessage->set_payload(ToProto<TProtobufString>(message.Payload));

    const auto streamSpecId = specStorage->GetStreamSpecId(message.PayloadSchema);
    protoMessage->set_stream_spec_id(ToProto(streamSpecId));
}

void FromProto(
    TMessage* message,
    const NProto::TMessage& protoMessage,
    const TStreamSpecsPtr& specStorage)
{
    YT_VERIFY(specStorage);

    message->MessageId = TMessageId(protoMessage.message_id());
    message->SystemTimestamp = FromProto<TSystemTimestamp>(protoMessage.system_timestamp());
    message->AlignmentTimestamp = FromProto<TSystemTimestamp>(protoMessage.alignment_timestamp());
    message->EventTimestamp = FromProto<TSystemTimestamp>(protoMessage.event_timestamp());
    message->Payload = FromProto<TPayload>(protoMessage.payload());

    const auto streamSpecId = FromProto<TStreamSpecId>(protoMessage.stream_spec_id());
    auto info = specStorage->GetStreamIdAndSchema(streamSpecId);
    message->StreamId = std::move(info.StreamId);
    message->PayloadSchema = std::move(info.Schema);
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TMessage& message, int columnId)
{
    return GetColumn(message.Payload, columnId);
}

NTableClient::TUnversionedValue GetColumn(const TMessage& message, TStringBuf columnName)
{
    return GetColumn(message.Payload, message.PayloadSchema, columnName);
}

////////////////////////////////////////////////////////////////////////////////

TMessageBuilder::TMessageBuilder(
    TStreamId streamId,
    NTableClient::TTableSchemaPtr schema,
    TMessageBuilder::TInitFunction init)
    : StreamId_(std::move(streamId))
    , Init_(std::move(init))
    , PayloadBuilder_(std::move(schema))
{
    Reset();
}

void TMessageBuilder::SetMessageId(TMessageId messageId)
{
    CurrentMessage_.MessageId = messageId;
}

void TMessageBuilder::SetSystemTimestamp(TSystemTimestamp systemTimestamp)
{
    CurrentMessage_.SystemTimestamp = systemTimestamp;
}

void TMessageBuilder::SetAlignmentTimestamp(TSystemTimestamp systemTimestamp)
{
    CurrentMessage_.AlignmentTimestamp = systemTimestamp;
}

void TMessageBuilder::SetEventTimestamp(TSystemTimestamp eventTimestamp)
{
    CurrentMessage_.EventTimestamp = eventTimestamp;
}

TPayloadBuilder& TMessageBuilder::Payload()
{
    return PayloadBuilder_;
}

const NTableClient::TTableSchemaPtr& TMessageBuilder::GetSchema() const
{
    return PayloadBuilder_.GetSchema();
}

TMessage TMessageBuilder::Finish()
{
    CurrentMessage_.StreamId = StreamId_;
    CurrentMessage_.Payload = PayloadBuilder_.Finish();
    CurrentMessage_.PayloadSchema = GetSchema();
    return std::exchange(CurrentMessage_, {});
}

void TMessageBuilder::Reset()
{
    CurrentMessage_ = {};
    PayloadBuilder_.Reset();
    if (Init_) {
        Init_(*this);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

TError DoValidateMessageMeta(const TMessageMeta& meta, TStringBuf fieldPrefix)
{
    if (meta.MessageId.Underlying().empty()) {
        return TError("%vMessageId is empty", fieldPrefix);
    }

    auto validateTimestamp = [&] (TSystemTimestamp timestamp, TStringBuf field) -> TError {
        if (timestamp == ZeroSystemTimestamp) {
            return TError("%v%v is undefined", fieldPrefix, field);
        }
        if (timestamp > MaxAdequateTimestamp) {
            if (timestamp == InfinitySystemTimestamp) {
                return TError("%v%v is infinity", fieldPrefix, field);
            }
            return TError("%v%v value %v is too large", fieldPrefix, field, TInstant::Seconds(timestamp.Underlying()));
        }
        return TError();
    };

    if (auto error = validateTimestamp(meta.SystemTimestamp, "SystemTimestamp"); !error.IsOK()) {
        return error;
    }
    if (auto error = validateTimestamp(meta.EventTimestamp, "EventTimestamp"); !error.IsOK()) {
        return error;
    }
    if (auto error = validateTimestamp(meta.AlignmentTimestamp, "AlignmentTimestamp"); !error.IsOK()) {
        return error;
    }
    return {};
}

TError DoValidateMessage(const TMessage& message, const TValidatePayloadOptions& options)
{
    if (auto error = DoValidateMessageMeta(message, /*fieldPrefix*/ "message."); !error.IsOK()) {
        return error;
    }
    if (options.ValidateValues) {
        if (GetMessageByteSize(message) > NTableClient::MaxStringValueLength) {
            return TError("Message is too big: size %v, limit %v",
                GetMessageByteSize(message),
                NTableClient::MaxStringValueLength);
        }
    }
    return DoValidatePayload("message.Payload", message.Payload, "message.PayloadSchema", message.PayloadSchema, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

void ValidateMessageMeta(const TMessageMeta& meta)
{
    auto error = DoValidateMessageMeta(meta, /*fieldPrefix*/ "");
    if (!error.IsOK()) {
        THROW_ERROR error;
    }
    if (meta.StreamId.Underlying().empty()) {
        THROW_ERROR_EXCEPTION("StreamId is empty");
    }
}

void ValidateMessage(const TMessage& message, const TValidatePayloadOptions& options)
{
    auto error = DoValidateMessage(message, options);
    if (!error.IsOK()) {
        THROW_ERROR error
            << TErrorAttribute("message_id", message.MessageId.Underlying())
            << TErrorAttribute("stream_id", message.StreamId);
    }
}

////////////////////////////////////////////////////////////////////////////////

TSystemTimestamp GetOrderingTimestamp(
    const TMessage& message,
    const TInputOrderingSpecPtr& orderingSpec)
{
    TSystemTimestamp timestamp;
    switch (orderingSpec->TimeType) {
        case ETimeType::EventTime:
            timestamp = message.EventTimestamp;
            break;
        case ETimeType::SystemTime:
            timestamp = message.SystemTimestamp;
            break;
        case ETimeType::CurrentTime:
            timestamp = TSystemTimestamp(TInstant::Now().Seconds());
            break;
    }
    return TSystemTimestamp(timestamp.Underlying() + GetOrDefault(orderingSpec->StreamDelays, message.StreamId).Seconds());
}

////////////////////////////////////////////////////////////////////////////////

const TMessageMeta& TInputMessage::GetMeta() const
{
    return *this;
}

TInputMessage::TInputMessage(TMessage&& message, TKey key)
    : TMessage(std::move(message))
    , Key(std::move(key))
    , ByteSize(GetMessageByteSize(*this))
{
    // We do soft validation to allow fix data.
    // Value validation mostly checks value sizes.
    ValidateMessage(*this, {.ValidateValues = false});
}

////////////////////////////////////////////////////////////////////////////////

const TMessageMeta& TOutputMessage::GetMeta() const
{
    return *this;
}

TOutputMessage::TOutputMessage(TMessage&& message, const TComputationStreamSpecStoragePtr& specStorage)
    : TMessage(std::move(message))
    , ByteSize(GetMessageByteSize(*this))
{
    YT_VERIFY(specStorage);
    ValidateMessage(*this);
    const auto expectedSchema = specStorage->GetSchema(StreamId);
    if (PayloadSchema.Get() != expectedSchema.Get() &&
        (!PayloadSchema || !expectedSchema || *PayloadSchema != *expectedSchema))
    {
        THROW_ERROR_EXCEPTION("message.PayloadSchema has unexpected value")
            << TErrorAttribute("schema", PayloadSchema ? NYson::ConvertToYsonString(*PayloadSchema) : NYson::TYsonString())
            << TErrorAttribute("expected_schema", expectedSchema ? NYson::ConvertToYsonString(*expectedSchema) : NYson::TYsonString())
            << TErrorAttribute("message_id", MessageId.Underlying())
            << TErrorAttribute("stream_id", StreamId);
    }
}

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TInputMessageConstPtr& message, TStringBuf /*spec*/)
{
    builder->AppendFormat("{MessageId: %v, Key: %v, SystemTimestamp: %v, EventTimestamp: %v, StreamId: %v, Payload: %v, PayloadSchema: %v}",
        message->MessageId,
        message->Key,
        message->SystemTimestamp,
        message->EventTimestamp,
        message->StreamId,
        message->Payload,
        message->PayloadSchema);
}

////////////////////////////////////////////////////////////////////////////////

NTableClient::TUnversionedValue GetColumn(const TInputMessageConstPtr& message, int columnId)
{
    return GetColumn(*message, columnId);
}

NTableClient::TUnversionedValue GetColumn(const TInputMessageConstPtr& message, TStringBuf columnName)
{
    return GetColumn(*message, columnName);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// Stack-buffer threshold for the message-id assembly fast path. Covers all realistic ids
// (16 hex digits + a stream id of ~30-50 chars + a few separators and a numeric offset).
// Longer ids fall back to a heap-allocated std::string.
constexpr size_t MessageIdStackBufferSize = 256;

// Write a fixed-width zero-padded lowercase hex representation of |value| (16 chars) into |out|
// and advance the pointer.
void WriteHex16(char*& out, ui64 value) noexcept
{
    static constexpr char kHexDigits[] = "0123456789abcdef";
    for (int i = 15; i >= 0; --i) {
        out[i] = kHexDigits[value & 0xF];
        value >>= 4;
    }
    out += 16;
}

void WriteBytes(char*& out, std::string_view bytes) noexcept
{
    std::memcpy(out, bytes.data(), bytes.size());
    out += bytes.size();
}

void WriteChar(char*& out, char c) noexcept
{
    *out++ = c;
}

// Picks a stack buffer when the id fits, otherwise falls back to std::string. |filler| writes
// |size| bytes into the provided buffer and returns a pointer to one-past-the-last written byte;
// the size must exactly match.
template <class TFiller>
TMessageId BuildMessageId(size_t size, TFiller&& filler)
{
    if (size <= MessageIdStackBufferSize) {
        char buffer[MessageIdStackBufferSize];
        char* end = filler(buffer);
        YT_VERIFY(end == buffer + size);
        return TMessageId(std::string_view(buffer, size));
    }
    std::string buffer(size, '\0');
    char* end = filler(buffer.data());
    YT_VERIFY(end == buffer.data() + size);
    return TMessageId(std::string_view(buffer));
}

} // namespace

TMessageId GenerateOrderedMessageId(const TUniqueSeqNo& uniqueSeqNo, const TStreamId& streamId, const TStringBuf& offset)
{
    const auto streamIdView = streamId.Underlying();
    const size_t size = 16 + 1 + streamIdView.size() + 1 + offset.size();
    return BuildMessageId(size, [&] (char* out) {
        WriteHex16(out, uniqueSeqNo.Underlying());
        WriteChar(out, '-');
        WriteBytes(out, streamIdView);
        WriteChar(out, ':');
        WriteBytes(out, std::string_view(offset.data(), offset.size()));
        return out;
    });
}

TMessageId GenerateOrderedMessageId(const TUniqueSeqNo& uniqueSeqNo, const TStreamId& streamId, const TStringBuf& offset, const TStringBuf& secondaryOffset)
{
    const auto streamIdView = streamId.Underlying();
    const size_t size = 16 + 1 + streamIdView.size() + 1 + offset.size() + 1 + secondaryOffset.size();
    return BuildMessageId(size, [&] (char* out) {
        WriteHex16(out, uniqueSeqNo.Underlying());
        WriteChar(out, '-');
        WriteBytes(out, streamIdView);
        WriteChar(out, ':');
        WriteBytes(out, std::string_view(offset.data(), offset.size()));
        WriteChar(out, ':');
        WriteBytes(out, std::string_view(secondaryOffset.data(), secondaryOffset.size()));
        return out;
    });
}

TMessageId GenerateInheritedMessageId(const TMessageId& sourceMessageId, const TStreamId& streamId, const TStringBuf& offset)
{
    const auto sourceView = sourceMessageId.Underlying();
    const auto streamIdView = streamId.Underlying();
    const size_t size = sourceView.size() + 1 + streamIdView.size() + 1 + offset.size();
    return BuildMessageId(size, [&] (char* out) {
        WriteBytes(out, sourceView);
        WriteChar(out, '-');
        WriteBytes(out, streamIdView);
        WriteChar(out, ':');
        WriteBytes(out, std::string_view(offset.data(), offset.size()));
        return out;
    });
}

////////////////////////////////////////////////////////////////////////////////

const TMessageId& TMessageHashMapOpsByMessageId::GetMessageId(const TMessageId& value) const
{
    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
