#pragma once

#include "common.h"
#include "snapshot.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSnapshotStore> TPtr;

    TSnapshotStore(Stroka location);

    TSnapshotReader::TPtr GetReader(i32 snapshotId);
    TSnapshotWriter::TPtr GetWriter(i32 snapshotId);

    // Returns NonexistingSnapshotId when no snapshots are found.
    i32 GetMaxSnapshotId();

private:
    Stroka Location;

    Stroka GetSnapshotFileName(i32 snapshotId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace
