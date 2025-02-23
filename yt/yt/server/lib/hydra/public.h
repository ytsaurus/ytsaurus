#pragma once

#include <yt/yt/server/lib/election/public.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

using NElection::TPeerIdSet;

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqResetStateHash;
class TMutationHeader;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IAutomaton)
DECLARE_REFCOUNTED_STRUCT(ISimpleHydraManager)
DECLARE_REFCOUNTED_STRUCT(IHydraManager)
DECLARE_REFCOUNTED_STRUCT(IDistributedHydraManager)
DECLARE_REFCOUNTED_STRUCT(IUpstreamSynchronizer)
DECLARE_REFCOUNTED_STRUCT(IPersistentResponseKeeper)
DECLARE_REFCOUNTED_STRUCT(IChangelog)
DECLARE_REFCOUNTED_STRUCT(IFileChangelog)
DECLARE_REFCOUNTED_STRUCT(IChangelogStore)
DECLARE_REFCOUNTED_STRUCT(IChangelogStoreFactory)
DECLARE_REFCOUNTED_STRUCT(IFileChangelogDispatcher)

struct ICheckpointableInputStream;
struct ICheckpointableOutputStream;

struct TSnapshotSaveContext;
struct TSnapshotLoadContext;

struct TSnapshotParams;
struct TRemoteSnapshotParams;
DECLARE_REFCOUNTED_STRUCT(ISnapshotReader)
DECLARE_REFCOUNTED_STRUCT(ISnapshotWriter)
DECLARE_REFCOUNTED_STRUCT(ISnapshotStore)

DECLARE_REFCOUNTED_CLASS(TSnapshotStoreThunk)
DECLARE_REFCOUNTED_CLASS(TChangelogStoreFactoryThunk)

DECLARE_REFCOUNTED_STRUCT(ILocalHydraJanitor)

DECLARE_REFCOUNTED_STRUCT(TSerializationDumperConfig)

struct TDistributedHydraManagerOptions;

class THydraContext;

class TMutation;
class TMutationContext;
struct TMutationRequest;
struct TMutationResponse;

DECLARE_REFCOUNTED_CLASS(TCompositeAutomaton)
DECLARE_REFCOUNTED_CLASS(TCompositeAutomatonPart)

class TEntityBase;
class TSaveContext;
class TLoadContext;

DECLARE_REFCOUNTED_STRUCT(TDistributedHydraManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicLocalHydraJanitorConfig)
DECLARE_REFCOUNTED_STRUCT(TDynamicDistributedHydraManagerConfig)
DECLARE_REFCOUNTED_STRUCT(TFileChangelogConfig)
DECLARE_REFCOUNTED_STRUCT(TFileChangelogDispatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TFileChangelogStoreConfig)
DECLARE_REFCOUNTED_STRUCT(THydraDryRunConfig)
DECLARE_REFCOUNTED_STRUCT(THydraJanitorConfig)
DECLARE_REFCOUNTED_STRUCT(TLocalHydraJanitorConfig)
DECLARE_REFCOUNTED_STRUCT(TLocalSnapshotStoreConfig)
DECLARE_REFCOUNTED_STRUCT(TRemoteChangelogStoreConfig)
DECLARE_REFCOUNTED_STRUCT(TRemoteSnapshotStoreConfig)
DECLARE_REFCOUNTED_CLASS(TSnapshotStoreConfigBase)

using TReign = int;

//! A special value representing an invalid snapshot (or changelog) id.
constexpr int InvalidSegmentId = -1;
constexpr int InvalidTerm = -1;
constexpr int InvalidReign = -1;

template <class TValue>
struct TDefaultEntityMapTraits;

template <
    class TValue,
    class TTraits = TDefaultEntityMapTraits<TValue>
>
class TEntityMap;

#define DECLARE_ENTITY_TYPE(entityType, keyType, hashType) \
    class entityType; \
    \
    struct TEntityTraitsImpl_##entityType \
    { \
        using TKey = keyType; \
        using THash = hashType; \
    }; \
    \
    inline TEntityTraitsImpl_##entityType GetEntityTraitsImpl(entityType*) \
    { \
        return TEntityTraitsImpl_##entityType(); \
    }

template <class T>
using TEntityTraits = decltype(GetEntityTraitsImpl(static_cast<T*>(nullptr)));

template <class T>
using TEntityKey = typename TEntityTraits<T>::TKey;

template <class T>
using TEntityHash = typename TEntityTraits<T>::THash;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EFinalRecoveryAction,
    (None)
    (BuildSnapshotAndRestart)
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
