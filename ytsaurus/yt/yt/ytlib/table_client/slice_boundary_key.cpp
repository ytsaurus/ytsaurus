#include "slice_boundary_key.h"

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSliceBoundaryKey::TSliceBoundaryKey(
    const TOwningKeyBound& keyBound,
    NChunkClient::TInputChunkPtr chunk,
    i64 dataWeight)
    : KeyBound_(std::move(keyBound))
    , DataWeight_(dataWeight)
    , Chunk_(chunk)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
