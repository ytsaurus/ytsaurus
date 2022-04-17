#include "snapshot_store_thunk.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr TSnapshotStoreThunk::CreateReader(int snapshotId)
{
    return Underlying_.Load()->CreateReader(snapshotId);
}

ISnapshotWriterPtr TSnapshotStoreThunk::CreateWriter(int snapshotId, const NProto::TSnapshotMeta& meta)
{
    return Underlying_.Load()->CreateWriter(snapshotId, meta);
}

TFuture<int> TSnapshotStoreThunk::GetLatestSnapshotId(int maxSnapshotId)
{
    return Underlying_.Load()->GetLatestSnapshotId(maxSnapshotId);
}

void TSnapshotStoreThunk::SetUnderlying(ISnapshotStorePtr underlying)
{
    Underlying_.Store(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
