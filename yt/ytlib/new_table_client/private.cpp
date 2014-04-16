#include "stdafx.h"
#include "private.h"

namespace NYT {
namespace NVersionedTableClient {

////////////////////////////////////////////////////////////////////////////////

const NLog::TLogger TableClientLogger("TableReader");

///////////////////////////////////////////////////////////////////////////////

int LowerBound(int lowerIndex, int upperIndex, std::function<bool(int)> less)
{
    while (upperIndex - lowerIndex > 0) {
        auto middle = (upperIndex + lowerIndex) / 2;
        if (less(middle)) {
            lowerIndex = middle + 1;
        } else {
            upperIndex = middle;
        }
    }
    return lowerIndex;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
