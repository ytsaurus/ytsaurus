#pragma once

#include <util/generic/hash_set.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TMlockStatistics
{
    THashSet<int> ErrorCodes;
    i64 BytesLockedSucessfully = 0;
    i64 BytesLockedUnsuccessfully = 0;
    i64 SuccessfullCallCount = 0;
    i64 UnsuccessfullCallCount = 0;
};

bool MlockFileMappings(bool populate = true, TMlockStatistics* statistics = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
