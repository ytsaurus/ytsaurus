#pragma once

#include <optional>

#include <util/system/types.h>
#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TOOMOptions
{
    std::optional<i64> MemoryLimit;
    TString HeapDumpPath = "oom.pb.gz";
};

void EnableEarlyOOMWatchdog(TOOMOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
