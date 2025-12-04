#pragma once

#include <yt/yt/core/misc/mex_set.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

//! Initializes itself with all sentinels. Useful because history has left us with
//! sentinels inside the valid medium index range.
class TUsedMediumIndexSet
    : public TMexIntSet
{
public:
    TUsedMediumIndexSet();
    void Clear();

private:
    void FillSentinels();
};
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
