#include "tags.h"

#include <bitset>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

using TEvent = NYT::NOrm::NServer::NObjects::NProto::TWatchRecord::TEvent;

constexpr int BitsPerBlock = 64;

////////////////////////////////////////////////////////////////////////////////

TTagSet FromProto(const TEvent::TChangedAttributeTags& attributeTags)
{
    TTagSet result;
    for (const auto& part : attributeTags.tags_mask_parts()) {
        std::bitset<BitsPerBlock> partBitSet{part.value()};
        for (int bit = 0; bit < BitsPerBlock; ++bit) {
            if (partBitSet[bit]) {
                result.insert(part.index() * BitsPerBlock + bit);
            }
        }
    }
    return result;
}

void ToProto(
    TEvent::TChangedAttributeTags& attributeTags,
    const TTagSet& tagsSet)
{
    TCompactFlatMap<ui64, std::bitset<BitsPerBlock>, 2> parts;
    for (TTag tag : tagsSet) {
        int value = tag.Underlying();
        parts[value / BitsPerBlock][value % BitsPerBlock] = true;
    }

    attributeTags.mutable_tags_mask_parts()->Reserve(parts.size());
    for (const auto& [index, value] : parts) {
        auto& protoPart = *attributeTags.add_tags_mask_parts();
        protoPart.set_index(index);
        protoPart.set_value(value.to_ullong());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
