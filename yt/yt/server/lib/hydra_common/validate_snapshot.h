#pragma once

#include "private.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TSnapshotValidationOptions
{
    bool SerializationDumpEnabled = false;
    bool EnableTotalWriteCountReport = false;
    TSerializationDumperConfigPtr DumpConfig;
};

////////////////////////////////////////////////////////////////////////////////

void ValidateSnapshot(
    const IAutomatonPtr& automaton,
    const ISnapshotReaderPtr& reader,
    const TSnapshotValidationOptions& options = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
