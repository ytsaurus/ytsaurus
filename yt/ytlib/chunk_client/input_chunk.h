#pragma once

#include "public.h"

#include <ytlib/misc/nullable.h>

#include <ytlib/ytree/attributes.h>
#include <ytlib/chunk_client/input_chunk.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedInputChunk
    : public TIntrinsicRefCounted
    , public NChunkClient::NProto::TInputChunk
{
    explicit TRefCountedInputChunk(
        const NProto::TInputChunk& other,
        int tableIndex = 0);

    explicit TRefCountedInputChunk(
        NProto::TInputChunk&& other,
        int tableIndex = 0);

    TRefCountedInputChunk(const TRefCountedInputChunk& other);
};

////////////////////////////////////////////////////////////////////////////////

//! Extracts various chunk statistics by first looking at
//! TSizeOverrideExt (if present) and then at TMiscExt.
void GetStatistics(
    const NProto::TInputChunk& chunk,
    i64* dataSize = nullptr,
    i64* rowCount = nullptr,
    i64* valueCount = nullptr);

//! Constructs a new chunk by slicing the original one and restricting
//! it to a given range. The original chunk may already contain non-trivial limits.
TRefCountedInputChunkPtr SliceChunk(
    TRefCountedInputChunkPtr inputChunk,
    const TNullable<NProto::TKey>& startKey = Null,
    const TNullable<NProto::TKey>& endKey = Null);

//! Tries to split the given chunk into #count
//! parts of almost equal size.
/*!
 *  May return less parts than requested.
 */
std::vector<TRefCountedInputChunkPtr> SliceChunkEvenly(
    TRefCountedInputChunkPtr inputChunk,
    int count);

//! Copies the data from #inputChunk into a new instance of TRefCountedInputChunk
//! and removes any limits.
TRefCountedInputChunkPtr CreateCompleteChunk(
    TRefCountedInputChunkPtr inputChunk);

////////////////////////////////////////////////////////////////////////////////

bool ExtractOverwriteFlag(const NYTree::IAttributeDictionary& attributes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

