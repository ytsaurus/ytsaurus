#include "shared_ref_helpers.h"

#include <ytlib/misc/foreach.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

size_t TotalLength(const std::vector<TSharedRef>& refs)
{
    size_t size = 0;
    FOREACH (const auto& ref, refs) {
        size += ref.Size();
    }
    return size;
}

TSharedRef MergeRefs(const std::vector<TSharedRef>& blocks) {
    TBlob result(TotalLength(blocks));
    size_t pos = 0;
    FOREACH(const auto& block, blocks) {
        std::copy(block.Begin(), block.End(), result.begin() + pos);
        pos += block.Size();
    }
    return TSharedRef(MoveRV(result));
}

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT
