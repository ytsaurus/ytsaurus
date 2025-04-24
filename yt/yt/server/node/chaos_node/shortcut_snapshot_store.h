#pragma once

#include "public.h"

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

struct TShortcutSnapshot
{
    NChaosClient::TReplicationEra Era = NChaosClient::InvalidReplicationEra;
};

struct IShortcutSnapshotStore
    : public virtual TRefCounted
{
public:
    virtual void UpdateShortcut(TChaosObjectId chaosObjectId, TShortcutSnapshot snapshot) = 0;
    virtual void RemoveShortcut(TChaosObjectId chaosObjectId) = 0;

    virtual std::optional<TShortcutSnapshot> FindShortcut(TChaosObjectId chaosObjectId) = 0;
    virtual TShortcutSnapshot GetShortcutOrThrow(TChaosObjectId chaosObjectId) = 0;

    virtual void Clear() = 0;
};

DEFINE_REFCOUNTED_TYPE(IShortcutSnapshotStore)

IShortcutSnapshotStorePtr CreateShortcutSnapshotStore();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
