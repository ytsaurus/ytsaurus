#include "compressed_integer_vector.h"

#include <yt/core/misc/zigzag.h>

namespace NYT::NTableChunkFormat {

////////////////////////////////////////////////////////////////////////////////

void PrepareDiffFromExpected(std::vector<ui32>* values, ui32* expected, ui32* maxDiff)
{
    if (values->empty()) {
        *expected = 0;
        *maxDiff = 0;
        return;
    }

    *expected = values->back() / values->size();

    *maxDiff = 0;
    i64 expectedValue = 0;
    for (int i = 0; i < values->size(); ++i) {
        expectedValue += *expected;
        i32 diff = values->at(i) - expectedValue;
        (*values)[i] = ZigZagEncode32(diff);
        *maxDiff = std::max(*maxDiff, (*values)[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
