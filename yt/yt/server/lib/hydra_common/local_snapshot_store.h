#pragma once

#include "public.h"

#include "snapshot.h"

namespace NYT::NHydra {

///////////////////////////////////////////////////////////////////////////////

// COMPAT(shakurov)
struct ILegacySnapshotStore
    : public ISnapshotStore
{
    virtual ISnapshotReaderPtr CreateRawReader(int snapshotId, i64 offset) = 0;
    virtual ISnapshotWriterPtr CreateRawWriter(int snapshotId) = 0;
};

DEFINE_REFCOUNTED_TYPE(ILegacySnapshotStore)

// COMPAT(shakurov): change return type to ISnapshotStorePtr after removing old Hydra.
ILegacySnapshotStorePtr CreateLocalSnapshotStore(TLocalSnapshotStoreConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr CreateUncompressedHeaderlessLocalSnapshotReader(const TString& fileName);
ISnapshotReaderPtr CreateLocalSnapshotReader(const TString& fileName, int snapshotId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
