#pragma once

#include "snapshot.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TSnapshotStoreThunk
    : public ISnapshotStore
{
public:
    virtual ISnapshotReaderPtr CreateReader(int snapshotId) override;
    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const NProto::TSnapshotMeta& meta) override;
    virtual TFuture<int> GetLatestSnapshotId(int maxSnapshotId) override;

    void SetUnderlying(ISnapshotStorePtr underlying);

private:
    TSpinLock SpinLock_;
    ISnapshotStorePtr Underlying_;


    ISnapshotStorePtr GetUnderlying();

};

DEFINE_REFCOUNTED_TYPE(TSnapshotStoreThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
