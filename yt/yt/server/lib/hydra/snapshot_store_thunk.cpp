#include "snapshot_store_thunk.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr TSnapshotStoreThunk::CreateReader(int snapshotId)
{
    return GetUnderlying()->CreateReader(snapshotId);
}

ISnapshotWriterPtr TSnapshotStoreThunk::CreateWriter(int snapshotId, const NProto::TSnapshotMeta& meta)
{
    return GetUnderlying()->CreateWriter(snapshotId, meta);
}

TFuture<int> TSnapshotStoreThunk::GetLatestSnapshotId(int maxSnapshotId)
{
    return GetUnderlying()->GetLatestSnapshotId(maxSnapshotId);
}

void TSnapshotStoreThunk::SetUnderlying(ISnapshotStorePtr underlying)
{
    TGuard<TSpinLock> guard(SpinLock_);
    Underlying_ = underlying;
}

ISnapshotStorePtr TSnapshotStoreThunk::GetUnderlying()
{
    TGuard<TSpinLock> guard(SpinLock_);
    return Underlying_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
