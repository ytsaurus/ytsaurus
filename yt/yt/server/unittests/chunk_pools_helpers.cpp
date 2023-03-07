#include "chunk_pools_helpers.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! We intentionally suppress the default output for TInputChunk as they are
//! identified by their addresses (already printed from the template function
//! for TIntrusivePtr) pretty well.
void PrintTo(const TInputChunk& /* chunk */, std::ostream* /* os */)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient:
