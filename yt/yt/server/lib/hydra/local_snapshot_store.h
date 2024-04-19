#pragma once

#include "public.h"

namespace NYT::NHydra {

///////////////////////////////////////////////////////////////////////////////

TFuture<ISnapshotStorePtr> CreateLocalSnapshotStore(
    TLocalSnapshotStoreConfigPtr config,
    IInvokerPtr ioInvoker);

ISnapshotReaderPtr CreateUncompressedHeaderlessLocalSnapshotReader(
    TString fileName,
    NProto::TSnapshotMeta meta,
    IInvokerPtr ioInvoker);
ISnapshotReaderPtr CreateLocalSnapshotReader(
    TString fileName,
    int snapshotId,
    IInvokerPtr ioInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
