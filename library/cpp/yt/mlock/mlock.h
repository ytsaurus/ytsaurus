#pragma once

#include <util/generic/hash_set.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TMlockStatistics
{
    THashSet<int> ErrorCodes;
    i64 BytesLockedSuccessfully = 0;
    i64 BytesLockedUnsuccessfully = 0;
    i64 SuccessfulCallCount = 0;
    i64 UnsuccessfulCallCount = 0;
};

bool MlockFileMappings(bool populate = true, TMlockStatistics* statistics = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
