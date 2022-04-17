#pragma once

#include "snapshot.h"

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotStoreThunk
    : public ISnapshotStore
{
public:
    ISnapshotReaderPtr CreateReader(int snapshotId) override;
    ISnapshotWriterPtr CreateWriter(int snapshotId, const NProto::TSnapshotMeta& meta) override;
    TFuture<int> GetLatestSnapshotId(int maxSnapshotId) override;

    void SetUnderlying(ISnapshotStorePtr underlying);

private:
    TAtomicObject<ISnapshotStorePtr> Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TSnapshotStoreThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
