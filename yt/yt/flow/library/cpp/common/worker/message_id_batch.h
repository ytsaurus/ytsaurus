#pragma once

#include <yt/yt/flow/library/cpp/common/public.h>

#include <library/cpp/yt/memory/ref.h>

#include <deque>

namespace NYT::NFlow::NWorker {

////////////////////////////////////////////////////////////////////////////////

//! Serialization of a batch of message ids into a single contiguous buffer.
//!
//! No redundant copying: the serializer first sums the exact upper bound of the buffer size,
//! allocates once and then fills it in a single pass.
//!
//! Each non-empty batch begins with a format version; on any incompatible change to the wire
//! format bump the version (see CurrentMessageIdBatchFormatVersion in message_id_batch.cpp) and
//! dispatch parsing by it.

////////////////////////////////////////////////////////////////////////////////

//! Serializes the pointed-to |messageIds| into one contiguous, versioned, self-contained buffer.
//! Takes pointers to avoid copying (and ref-counting) the ids on the hot path.
//! Returns an empty ref for an empty batch.
TSharedRef SerializeMessageIdBatch(const std::deque<const TMessageId*>& messageIds);

//! Parses a buffer produced by |SerializeMessageIdBatch| back into message ids.
//! String data is copied into each id's own storage, so |buffer| may be released afterwards.
//! Throws on an unknown version or a truncated/malformed buffer.
std::deque<TMessageId> ParseMessageIdBatch(TRef buffer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NWorker
