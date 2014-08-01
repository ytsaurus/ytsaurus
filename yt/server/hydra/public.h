#pragma once

#include <core/misc/common.h>

#include <ytlib/hydra/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IAutomaton)
DECLARE_REFCOUNTED_STRUCT(IHydraManager)

DECLARE_REFCOUNTED_STRUCT(IChangelog)
DECLARE_REFCOUNTED_STRUCT(IChangelogStore)

DECLARE_REFCOUNTED_CLASS(TFileChangelogDispatcher)

struct TSnapshotParams;
DECLARE_REFCOUNTED_STRUCT(ISnapshotReader)
DECLARE_REFCOUNTED_STRUCT(ISnapshotWriter)
DECLARE_REFCOUNTED_STRUCT(ISnapshotStore)
DECLARE_REFCOUNTED_CLASS(TFileSnapshotStore)
DECLARE_REFCOUNTED_CLASS(TFileChangelog)

struct TMutationRequest;
struct TMutationResponse;
class TMutationContext; 

DECLARE_REFCOUNTED_CLASS(TCompositeAutomaton)
DECLARE_REFCOUNTED_CLASS(TCompositeAutomatonPart)

DECLARE_REFCOUNTED_CLASS(TMutation)

class TSaveContext;
class TLoadContext;

DECLARE_REFCOUNTED_CLASS(TFileChangelogConfig)
DECLARE_REFCOUNTED_CLASS(TFileChangelogStoreConfig)
DECLARE_REFCOUNTED_CLASS(TLocalSnapshotStoreConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteSnapshotStoreConfig)
DECLARE_REFCOUNTED_CLASS(TSnapshotDownloaderConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreConfig)
DECLARE_REFCOUNTED_CLASS(TChangelogDownloaderConfig)
DECLARE_REFCOUNTED_CLASS(TFollowerTrackerConfig)
DECLARE_REFCOUNTED_CLASS(TLeaderCommitterConfig)
DECLARE_REFCOUNTED_CLASS(TDistributedHydraManagerConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
