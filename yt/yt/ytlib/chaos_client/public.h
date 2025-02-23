#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

struct TAlienCellDescriptorLite;
struct TAlienPeerDescriptor;
struct TAlienCellDescriptor;

DECLARE_REFCOUNTED_STRUCT(IChaosCellDirectorySynchronizer)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IChaosCellChannelFactory)
DECLARE_REFCOUNTED_STRUCT(IChaosResidencyCache)
DECLARE_REFCOUNTED_STRUCT(IBannedReplicaTracker)
DECLARE_REFCOUNTED_STRUCT(IBannedReplicaTrackerCache)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardsWatcher)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardsWatcherClient)
DECLARE_REFCOUNTED_STRUCT(IReplicationCardWatcherCallbacks)
DECLARE_REFCOUNTED_STRUCT(TChaosCellDirectorySynchronizerConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosResidencyCacheConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationCardChannelConfig)
DECLARE_REFCOUNTED_STRUCT(TReplicationCardsWatcherConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
