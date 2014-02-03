#pragma once

#include <core/misc/common.h>
#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

#include <ytlib/hydra/public.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

struct TChangelogCreateParams;

struct IChangelog;
typedef TIntrusivePtr<IChangelog> IChangelogPtr;

struct IChangelogStore;
typedef TIntrusivePtr<IChangelogStore> IChangelogStorePtr;

struct IChangelogCatalog;
typedef TIntrusivePtr<IChangelogCatalog> IChangelogCatalogPtr;

struct ISnapshotReader;
typedef TIntrusivePtr<ISnapshotReader> ISnapshotReaderPtr;

struct ISnapshotWriter;
typedef TIntrusivePtr<ISnapshotWriter> ISnapshotWriterPtr;

struct ISnapshotStore;
typedef TIntrusivePtr<ISnapshotStore> ISnapshotStorePtr;

struct ISnapshotCatalog;
typedef TIntrusivePtr<ISnapshotCatalog> ISnapshotCatalogPtr;

struct TMutationRequest;
struct TMutationResponse;
class TMutationContext; 

struct IAutomaton;
typedef TIntrusivePtr<IAutomaton> IAutomatonPtr;

struct IHydraManager;
typedef TIntrusivePtr<IHydraManager> IHydraManagerPtr;

class TCompositeAutomaton;
typedef TIntrusivePtr<TCompositeAutomaton> TCompositeAutomatonPtr;

class TCompositeAutomatonPart;
typedef TIntrusivePtr<TCompositeAutomatonPart> TCompositeAutomatonPartPtr;

class TMutation;
typedef TIntrusivePtr<TMutation> TMutationPtr;

class TSaveContext;
class TLoadContext;

class TFileChangelogConfig;
typedef TIntrusivePtr<TFileChangelogConfig> TFileChangelogConfigPtr;

class TFileChangelogStoreConfig;
typedef TIntrusivePtr<TFileChangelogStoreConfig> TFileChangelogStoreConfigPtr;

class TMutiplexedFileChangelogConfig;
typedef TIntrusivePtr<TMutiplexedFileChangelogConfig> TMutiplexedFileChangelogConfigPtr;

class TFileChangelogCatalogConfig;
typedef TIntrusivePtr<TFileChangelogCatalogConfig> TFileChangelogCatalogConfigPtr;

class TFileSnapshotStoreConfig;
typedef TIntrusivePtr<TFileSnapshotStoreConfig> TFileSnapshotStoreConfigPtr;

typedef TFileSnapshotStoreConfig TFileSnapshotCatalogConfig;
typedef TIntrusivePtr<TFileSnapshotCatalogConfig> TFileSnapshotCatalogConfigPtr;

class TSnapshotDownloaderConfig;
typedef TIntrusivePtr<TSnapshotDownloaderConfig> TSnapshotDownloaderConfigPtr;

class TChangelogDownloaderConfig;
typedef TIntrusivePtr<TChangelogDownloaderConfig> TChangelogDownloaderConfigPtr;

class TFollowerTrackerConfig;
typedef TIntrusivePtr<TFollowerTrackerConfig> TFollowerTrackerConfigPtr;

class TLeaderCommitterConfig;
typedef TIntrusivePtr<TLeaderCommitterConfig> TLeaderCommitterConfigPtr;

class TDistributedHydraManagerConfig;
typedef TIntrusivePtr<TDistributedHydraManagerConfig> TDistributedHydraManagerConfigPtr;

////////////////////////////////////////////////////////////////////////////////

extern TLazyIntrusivePtr<NConcurrency::TActionQueue> HydraIOQueue;

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
