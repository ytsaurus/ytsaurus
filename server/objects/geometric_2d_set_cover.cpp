#include "geometric_2d_set_cover.h"

namespace NYP::NServer::NObjects::NGeometric2DSetCover::NPrivate {

////////////////////////////////////////////////////////////////////////////////

std::vector<int> GetNonZeroBits(const TDynBitMap& bitmap)
{
    std::vector<int> result;
    result.reserve(bitmap.Count());
    for (auto bit = bitmap.FirstNonZeroBit();
        bit < bitmap.Size();
        bit = bitmap.NextNonZeroBit(bit))
    {
        result.push_back(bit);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects::NGeometric2DSetCover::NPrivate
