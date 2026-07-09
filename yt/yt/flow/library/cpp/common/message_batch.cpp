#include "message_batch.h"

#include "stream_spec_storage.h"

#include <yt/yt/flow/library/cpp/misc/compact_unversioned_owning_row.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/coding/varint.h>

namespace NYT::NFlow {

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TMessageBatchTag
{ };

// Batch layout:
//   [varint format_version][record][record]...   (empty batches carry nothing)
// Record layout:
//   [fixed u64 system_timestamp][fixed u64 alignment_timestamp][fixed u64 event_timestamp]
//   [varint stream_spec_id][varint message_id_length][message_id bytes][payload (row wire format)]
// Records are not self-describing. On any incompatible change bump CurrentMessageBatchFormatVersion
// and dispatch parsing by the version read from the batch header.
constexpr ui32 CurrentMessageBatchFormatVersion = 1;

// Throws if fewer than |size| bytes remain in |[current, end)|.
// Guards record parsing against a truncated or malformed buffer.
void ValidateBatchNotTruncated(const char* current, const char* end, size_t size)
{
    YT_VERIFY(current <= end);
    THROW_ERROR_EXCEPTION_IF(
        static_cast<size_t>(end - current) < size,
        "Message batch record is truncated or malformed");
}

char* WriteFormatVersion(char* dst)
{
    return dst + WriteVarUint32(dst, CurrentMessageBatchFormatVersion);
}

// Reads and validates the leading format version. An empty batch carries no records and
// no version. Throws on an unknown version.
const char* ReadFormatVersion(const char* begin, const char* end)
{
    if (begin == end) {
        return begin;
    }
    ui32 version;
    const char* current = begin + ReadVarUint32(begin, end, &version);
    THROW_ERROR_EXCEPTION_UNLESS(
        version == CurrentMessageBatchFormatVersion,
        "Unsupported message batch format version %v (expected %v)",
        version,
        CurrentMessageBatchFormatVersion);
    return current;
}

char* WriteMessageRecord(char* dst, const TMessage& message, const TStreamSpecsPtr& specStorage)
{
    char* current = dst;

    auto writeFixed = [&] (ui64 value) {
        ::memcpy(current, &value, sizeof(value));
        current += sizeof(value);
    };
    writeFixed(ToProto(message.SystemTimestamp));
    writeFixed(ToProto(message.AlignmentTimestamp));
    writeFixed(ToProto(message.EventTimestamp));

    const auto streamSpecId = specStorage->GetStreamSpecId(message.PayloadSchema);
    current += WriteVarInt64(current, ToProto(streamSpecId));

    const auto& messageId = message.MessageId.Underlying();
    current += WriteVarUint32(current, messageId.size());
    ::memcpy(current, messageId.data(), messageId.size());
    current += messageId.size();

    current = SerializeToBuffer(current, message.Payload.Underlying());

    return current;
}

const char* ReadMessageRecord(const char* begin, const char* end, TMessage* message, const TStreamSpecsPtr& specStorage)
{
    const char* current = begin;

    ValidateBatchNotTruncated(current, end, 3 * sizeof(ui64));
    auto readFixed = [&] {
        ui64 value;
        ::memcpy(&value, current, sizeof(value));
        current += sizeof(value);
        return value;
    };
    message->SystemTimestamp = FromProto<TSystemTimestamp>(readFixed());
    message->AlignmentTimestamp = FromProto<TSystemTimestamp>(readFixed());
    message->EventTimestamp = FromProto<TSystemTimestamp>(readFixed());

    i64 streamSpecIdValue;
    current += ReadVarInt64(current, end, &streamSpecIdValue);
    const auto streamSpecId = FromProto<TStreamSpecId>(streamSpecIdValue);
    auto info = specStorage->GetStreamIdAndSchema(streamSpecId);
    message->StreamId = std::move(info.StreamId);
    message->PayloadSchema = std::move(info.Schema);

    ui32 messageIdLength;
    current += ReadVarUint32(current, end, &messageIdLength);
    ValidateBatchNotTruncated(current, end, messageIdLength);
    message->MessageId = TMessageId(TStringBuf(current, messageIdLength));
    current += messageIdLength;

    TCompactUnversionedOwningRow payload;
    current = DeserializeFromBuffer(current, end, &payload);
    message->Payload = TPayload(std::move(payload));

    YT_VERIFY(current <= end);
    return current;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TMessageBatchSerializer::TMessageBatchSerializer(TStreamSpecsPtr specStorage)
    : SpecStorage_(std::move(specStorage))
{
    YT_VERIFY(SpecStorage_);
    // Every batch starts with the format version header, even an empty one. This keeps the
    // format uniform and leaves room to attach batch-level metadata after the version later.
    ByteSize_ = MaxVarUint32Size;
}

i64 TMessageBatchSerializer::GetMessageWireSize(const TMessage& message)
{
    return 3 * sizeof(ui64) +                                      // system/alignment/event timestamps.
        MaxVarInt64Size +                                          // stream_spec_id.
        MaxVarUint32Size + message.MessageId.Underlying().size() + // message id.
        GetWireByteSize(message.Payload.Underlying());             // payload.
}

void TMessageBatchSerializer::AddMessage(TOutputMessageConstPtr message)
{
    const auto messageWireSize = GetMessageWireSize(*message);
    AddMessage(std::move(message), messageWireSize);
}

void TMessageBatchSerializer::AddMessage(TOutputMessageConstPtr message, i64 messageWireSize)
{
    ByteSize_ += messageWireSize;
    Messages_.push_back(std::move(message));
}

bool TMessageBatchSerializer::IsEmpty() const
{
    return Messages_.empty();
}

int TMessageBatchSerializer::GetMessageCount() const
{
    return std::ssize(Messages_);
}

i64 TMessageBatchSerializer::GetByteSize() const
{
    return ByteSize_;
}

TSharedRef TMessageBatchSerializer::Finish()
{
    // ByteSize_ is an exact upper bound: the version header plus each message's wire size.
    // Even an empty batch yields the version header, never an empty buffer.
    auto buffer = TSharedMutableRef::Allocate<TMessageBatchTag>(ByteSize_, {.InitializeStorage = false});
    char* current = WriteFormatVersion(buffer.Begin());
    for (const auto& message : Messages_) {
        current = WriteMessageRecord(current, *message, SpecStorage_);
    }
    auto result = buffer.Slice(buffer.Begin(), current);

    Messages_.clear();
    ByteSize_ = MaxVarUint32Size;
    return result;
}

std::deque<TMessage> ParseMessageBatch(
    TRef buffer,
    const TStreamSpecsPtr& specStorage)
{
    YT_VERIFY(specStorage);

    std::deque<TMessage> messages;
    const char* current = ReadFormatVersion(buffer.Begin(), buffer.End());
    while (current != buffer.End()) {
        TMessage message;
        current = ReadMessageRecord(current, buffer.End(), &message, specStorage);
        messages.push_back(std::move(message));
    }
    return messages;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
