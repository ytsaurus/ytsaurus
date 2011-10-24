#pragma once

#include "common.h"
#include "snapshot.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotStore
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TSnapshotStore> TPtr;

    TSnapshotStore(Stroka location);

    TSnapshotReader::TPtr GetReader(i32 snapshotId);
    TSnapshotWriter::TPtr GetWriter(i32 snapshotId);

    //! \return The largest id of the snapshot that exists locally
    //! or #NonexistingSnapshotId if no snapshots are found.
    i32 GetMaxSnapshotId();

private:
    Stroka Location;

    Stroka GetSnapshotFileName(i32 snapshotId);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
