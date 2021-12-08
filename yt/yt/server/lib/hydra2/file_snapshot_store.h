#pragma once

#include "private.h"

#include <yt/yt/server/lib/hydra_common/public.h>

#include <yt/yt/core/compression/public.h>

#include <yt/yt/core/misc/optional.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

class TFileSnapshotStore
    : public TRefCounted
{
public:
    explicit TFileSnapshotStore(NHydra::TLocalSnapshotStoreConfigPtr config);
    void Initialize();

    ~TFileSnapshotStore();

    bool CheckSnapshotExists(int snapshotId);
    int GetLatestSnapshotId(int maxSnapshotId);

    NHydra::ISnapshotReaderPtr CreateReader(int snapshotId);
    NHydra::ISnapshotReaderPtr CreateRawReader(int snapshotId, i64 offset);

    NHydra::ISnapshotWriterPtr CreateWriter(int snapshotId, const NHydra::NProto::TSnapshotMeta& meta);
    NHydra::ISnapshotWriterPtr CreateRawWriter(int snapshotId);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TFileSnapshotStore)

////////////////////////////////////////////////////////////////////////////////

NHydra::ISnapshotReaderPtr CreateFileSnapshotReader(
    const TString& fileName,
    int snapshotId,
    bool raw,
    std::optional<i64> offset = std::nullopt,
    bool skipHeader = false);

NHydra::ISnapshotWriterPtr CreateFileSnapshotWriter(
    const TString& fileName,
    NCompression::ECodec codec,
    int snapshotId,
    const NHydra::NProto::TSnapshotMeta& meta,
    bool raw);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra2
