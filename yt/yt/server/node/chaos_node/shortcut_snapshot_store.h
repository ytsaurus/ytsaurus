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
    virtual void UpdateShortcut(TReplicationCardId replicationCardId, TShortcutSnapshot snapshot) = 0;
    virtual void RemoveShortcut(TReplicationCardId replicationCardId) = 0;

    virtual std::optional<TShortcutSnapshot> FindShortcut(TReplicationCardId replicationCardId) = 0;
    virtual TShortcutSnapshot GetShortcutOrThrow(TReplicationCardId replicationCardId) = 0;

    virtual void Clear() = 0;
};

DEFINE_REFCOUNTED_TYPE(IShortcutSnapshotStore)

IShortcutSnapshotStorePtr CreateShortcutSnapshotStore();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
