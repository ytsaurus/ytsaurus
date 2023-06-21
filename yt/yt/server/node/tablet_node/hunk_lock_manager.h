#pragma once

#include "public.h"

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

struct IHunkLockManager
    : public TRefCounted
{
    virtual void Initialize() = 0;

    virtual void StartEpoch() = 0;
    virtual void StopEpoch() = 0;

    virtual void RegisterHunkStore(
        THunkStoreId hunkStoreId,
        TCellId hunkCellId,
        TTabletId hunkTabletId,
        NHydra::TRevision hunkMountRevision) = 0;
    virtual void UnregisterHunkStore(THunkStoreId hunkStoreId) = 0;

    virtual void OnBoggleLockPrepared(THunkStoreId hunkStoreId, bool lock) = 0;
    virtual void OnBoggleLockAborted(THunkStoreId hunkStoreId, bool lock) = 0;

    virtual TFuture<void> LockHunkStores(const NTableClient::THunkChunksInfo& hunkChunksInfo) = 0;

    virtual void IncrementPersistentLockCount(THunkStoreId hunkStoreId, int count) = 0;
    virtual void IncrementTransientLockCount(THunkStoreId hunkStoreId, int count) = 0;

    virtual int GetTotalLockedHunkStoreCount() const = 0;

    virtual std::optional<int> GetPersistentLockCount(THunkStoreId hunkStoreId) = 0;
    virtual std::optional<int> GetTotalLockCount(THunkStoreId hunkStoreId) = 0;

    virtual void Save(TSaveContext& context) const = 0;
    virtual void Load(TLoadContext& context) = 0;

    virtual void BuildOrchid(NYTree::TFluentAny consumer) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IHunkLockManager)

////////////////////////////////////////////////////////////////////////////////

IHunkLockManagerPtr CreateHunkLockManager(
    TTablet* tablet,
    ITabletContext* context);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
