#pragma once

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

struct IFileSnapshotStore
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual bool CheckSnapshotExists(int snapshotId) = 0;
    virtual int GetLatestSnapshotId(int maxSnapshotId) = 0;

    virtual ISnapshotReaderPtr CreateReader(int snapshotId) = 0;
    virtual ISnapshotReaderPtr CreateRawReader(int snapshotId, i64 offset) = 0;

    virtual ISnapshotWriterPtr CreateWriter(int snapshotId, const NProto::TSnapshotMeta& meta) = 0;
    virtual ISnapshotWriterPtr CreateRawWriter(int snapshotId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IFileSnapshotStore)

IFileSnapshotStorePtr CreateFileSnapshotStore(TLocalSnapshotStoreConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

ISnapshotReaderPtr CreateFileSnapshotReader(
    const TString& fileName,
    int snapshotId,
    bool raw,
    std::optional<i64> offset = {},
    bool skipHeader = false);

ISnapshotWriterPtr CreateFileSnapshotWriter(
    const TString& fileName,
    NCompression::ECodec codec,
    int snapshotId,
    const NProto::TSnapshotMeta& meta,
    bool raw);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
