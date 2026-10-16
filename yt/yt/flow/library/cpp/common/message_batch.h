#pragma once

#include "public.h"

#include "message.h"

#include <library/cpp/yt/memory/ref.h>

#include <deque>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Serialization of a batch of output messages into a single contiguous buffer.
//!
//! Usage: append messages with |AddMessage|, then call |Finish| to obtain the serialized buffer.
//! |Finish| empties the serializer, so the same instance can immediately build the next batch.
//! |GetByteSize| reports the running serialized size at any point, which lets a caller split a
//! long message stream into size-bounded batches without a separate measuring pass.
//!
//! No redundant copying: each appended message contributes its exact wire size to a running
//! total, so |Finish| allocates once and fills the buffer in a single pass without re-measuring.
//!
//! The wire format (including its version header) is fully owned by this serializer; callers
//! never deal with the version or the framing directly. On any incompatible change to the
//! format bump the version (see CurrentMessageBatchFormatVersion in message_batch.cpp) and
//! dispatch parsing by it.
class TMessageBatchSerializer
{
public:
    explicit TMessageBatchSerializer(TStreamSpecsPtr specStorage);

    //! Wire size |message| will occupy in a batch (excludes the per-batch version header).
    //! Exposed so callers that split a message stream into size-bounded batches can take the
    //! decision before appending; pass the result to |AddMessage| to avoid recomputing it.
    static i64 GetMessageWireSize(const TMessage& message);

    //! Appends |message|, folding its wire size into the running total. The size is computed once.
    void AddMessage(TOutputMessageConstPtr message);
    //! Same, reusing a wire size already obtained from |GetMessageWireSize(*message)|.
    void AddMessage(TOutputMessageConstPtr message, i64 messageWireSize);

    bool IsEmpty() const;
    int GetMessageCount() const;
    //! Current serialized size in bytes, including the per-batch version header.
    i64 GetByteSize() const;

    //! Serializes everything appended so far into one contiguous, versioned, self-contained
    //! buffer and resets the serializer for reuse. Even an empty batch carries the version header,
    //! so the result is never an empty ref.
    TSharedRef Finish();

private:
    const TStreamSpecsPtr SpecStorage_;
    std::deque<TOutputMessageConstPtr> Messages_;
    i64 ByteSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Parses a buffer produced by |TMessageBatchSerializer| back into raw messages.
//! String data is copied into each message's own storage, so |buffer| may be released
//! afterwards. Throws on an unknown version or a truncated/malformed buffer.
std::deque<TMessage> ParseMessageBatch(
    TRef buffer,
    const TStreamSpecsPtr& specStorage);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
