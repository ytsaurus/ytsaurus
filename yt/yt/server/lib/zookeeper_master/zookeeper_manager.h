#pragma once

#include "public.h"

namespace NYT::NZookeeperMaster {

////////////////////////////////////////////////////////////////////////////////

struct IZookeeperManager
    : public virtual TRefCounted
{
    // Common stuff.

    //! Corresponds to automaton part clear.
    virtual void Clear() = 0;

    // Shard management.

    //! Registers new zookeeper shard. This function is called either
    //! on zookeeper shard creation or during snapshot loading.
    virtual void RegisterShard(TZookeeperShard* shard) = 0;

    //! Unregisters zookeeper shard. This function is called when
    //! zookeeper shard is destroyed.
    virtual void UnregisterShard(TZookeeperShard* shard) = 0;

    //! Returns true if there is a registered root zookeeper shard
    //! and false otherwise.
    virtual bool HasRootShard() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IZookeeperManager)

////////////////////////////////////////////////////////////////////////////////

IZookeeperManagerPtr CreateZookeeperManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperMaster
