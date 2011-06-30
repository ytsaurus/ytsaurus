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

    TAutoPtr<TSnapshotReader> GetReader(i32 segmentId);
    TAutoPtr<TSnapshotWriter> GetWriter(i32 segmentId);

    i32 GetMaxSnapshotId();

private:
    Stroka Location;

    Stroka GetSnapshotFileName(i32 segmentId);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
