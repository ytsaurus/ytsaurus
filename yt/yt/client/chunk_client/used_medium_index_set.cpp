#include "used_medium_index_set.h"

#include "public.h"

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

TUsedMediumIndexSet::TUsedMediumIndexSet()
    : TMexIntSet()
{
    FillSentinels();
}

void TUsedMediumIndexSet::Clear()
{
    TMexIntSet::Clear();
    FillSentinels();
}

void TUsedMediumIndexSet::FillSentinels()
{
    for (auto mediumIndex : GetSentinelMediumIndexes()) {
        Insert(mediumIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
