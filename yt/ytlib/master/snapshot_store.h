#pragma once

#include "common.h"
#include "snapshot.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO: make refcounted
class TSnapshotStore
{
public:
    TSnapshotStore(Stroka location);

    TSnapshotReader::TPtr GetReader(i32 segmentId);
    TSnapshotWriter::TPtr GetWriter(i32 segmentId);

    i32 GetMaxSnapshotId();

private:
    Stroka Location;

    Stroka GetSnapshotFileName(i32 segmentId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
