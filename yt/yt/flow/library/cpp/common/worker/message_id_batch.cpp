#include "message_id_batch.h"

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/coding/varint.h>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TMessageIdBatchTag
{ };

// Batch layout:
//   [varint format_version][record][record]...   (empty batches carry nothing)
// Record layout:
//   [varint message_id_length][message_id bytes]
// On any incompatible change bump CurrentMessageIdBatchFormatVersion and dispatch parsing
// by the version read from the batch header.
constexpr ui32 CurrentMessageIdBatchFormatVersion = 1;

// Throws if fewer than |size| bytes remain in |[current, end)|.
void ValidateBatchNotTruncated(const char* current, const char* end, size_t size)
{
    YT_VERIFY(current <= end);
    THROW_ERROR_EXCEPTION_IF(
        static_cast<size_t>(end - current) < size,
        "Message id batch record is truncated or malformed");
}

char* WriteFormatVersion(char* dst)
{
    return dst + WriteVarUint32(dst, CurrentMessageIdBatchFormatVersion);
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
        version == CurrentMessageIdBatchFormatVersion,
        "Unsupported message id batch format version %v (expected %v)",
        version,
        CurrentMessageIdBatchFormatVersion);
    return current;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMessageIdBatch(const std::deque<const TMessageId*>& messageIds)
{
    if (messageIds.empty()) {
        return {};
    }

    size_t size = MaxVarUint32Size;
    for (const auto* messageId : messageIds) {
        size += MaxVarUint32Size + messageId->Underlying().size();
    }

    auto buffer = TSharedMutableRef::Allocate<TMessageIdBatchTag>(size, {.InitializeStorage = false});
    char* current = WriteFormatVersion(buffer.Begin());
    for (const auto* messageId : messageIds) {
        const auto& underlying = messageId->Underlying();
        current += WriteVarUint32(current, underlying.size());
        ::memcpy(current, underlying.data(), underlying.size());
        current += underlying.size();
    }
    return buffer.Slice(buffer.Begin(), current);
}

std::deque<TMessageId> ParseMessageIdBatch(TRef buffer)
{
    std::deque<TMessageId> messageIds;
    const char* current = ReadFormatVersion(buffer.Begin(), buffer.End());
    while (current != buffer.End()) {
        ui32 length;
        current += ReadVarUint32(current, buffer.End(), &length);
        ValidateBatchNotTruncated(current, buffer.End(), length);
        messageIds.push_back(TMessageId(TStringBuf(current, length)));
        current += length;
    }
    return messageIds;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
