#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotValidationOptions
{
    bool SerializationDumpEnabled = false;
    bool EnableTotalWriteCountReport = false;
    TSerializationDumperConfigPtr DumpConfig;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
