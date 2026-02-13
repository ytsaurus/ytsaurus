#pragma once

#include "public.h"

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

TFuture<ISnapshotStorePtr> CreateLocalSnapshotStore(
    TLocalSnapshotStoreConfigPtr config,
    IInvokerPtr ioInvoker);

ISnapshotReaderPtr CreateUncompressedHeaderlessLocalSnapshotReader(
    std::string fileName,
    NProto::TSnapshotMeta meta,
    IInvokerPtr ioInvoker);
ISnapshotReaderPtr CreateLocalSnapshotReader(
    std::string fileName,
    int snapshotId,
    IInvokerPtr ioInvoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
