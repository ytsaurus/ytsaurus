#pragma once

#include "snapshot.h"

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
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    ISnapshotStorePtr Underlying_;


    ISnapshotStorePtr GetUnderlying();

};

DEFINE_REFCOUNTED_TYPE(TSnapshotStoreThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
