#pragma once

#include <yt/ytlib/hydra/public.h>

#include <yt/core/misc/public.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IAutomaton)
DECLARE_REFCOUNTED_STRUCT(IHydraManager)

DECLARE_REFCOUNTED_STRUCT(IChangelog)
DECLARE_REFCOUNTED_STRUCT(IChangelogStore)
DECLARE_REFCOUNTED_STRUCT(IChangelogStoreFactory)

DECLARE_REFCOUNTED_CLASS(TFileChangelogDispatcher)

struct TSnapshotParams;
DECLARE_REFCOUNTED_STRUCT(ISnapshotReader)
DECLARE_REFCOUNTED_STRUCT(ISnapshotWriter)
DECLARE_REFCOUNTED_STRUCT(ISnapshotStore)
DECLARE_REFCOUNTED_STRUCT(ISnapshotStore)
DECLARE_REFCOUNTED_CLASS(TFileSnapshotStore)
DECLARE_REFCOUNTED_CLASS(TFileChangelog)

DECLARE_REFCOUNTED_CLASS(TSnapshotStoreThunk)
DECLARE_REFCOUNTED_CLASS(TChangelogStoreFactoryThunk)

struct TDistributedHydraManagerOptions;

class TMutation;
class TMutationContext;
struct TMutationRequest;
struct TMutationResponse;

DECLARE_REFCOUNTED_CLASS(TCompositeAutomaton)
DECLARE_REFCOUNTED_CLASS(TCompositeAutomatonPart)

class TEntityBase;
class TSaveContext;
class TLoadContext;

DECLARE_REFCOUNTED_CLASS(TFileChangelogConfig)
DECLARE_REFCOUNTED_CLASS(TFileChangelogDispatcherConfig)
DECLARE_REFCOUNTED_CLASS(TFileChangelogStoreConfig)
DECLARE_REFCOUNTED_CLASS(TLocalSnapshotStoreConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteSnapshotStoreConfig)
DECLARE_REFCOUNTED_CLASS(TRemoteChangelogStoreConfig)
DECLARE_REFCOUNTED_CLASS(TDistributedHydraManagerConfig)

//! A special value representing an invalid snapshot (or changelog) id.
const int InvalidSegmentId = -1;

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

} // namespace NYT::NHydra
