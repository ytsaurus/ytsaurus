#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/client/table_client/key_bound.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

class TSliceBoundaryKey
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TOwningKeyBound, KeyBound);
    DEFINE_BYVAL_RO_PROPERTY(i64, DataWeight);
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TInputChunkPtr, Chunk);

public:
    TSliceBoundaryKey(
        const TOwningKeyBound& keyBound,
        NChunkClient::TInputChunkPtr chunk,
        i64 dataWeight);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
