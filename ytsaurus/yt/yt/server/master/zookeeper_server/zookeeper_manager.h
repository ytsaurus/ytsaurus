#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

namespace NYT::NZookeeperServer {

////////////////////////////////////////////////////////////////////////////////

struct IZookeeperManager
    : public virtual TRefCounted
{
    // Common stuff.

    virtual void Initialize() = 0;

    // Zookeeper shard management.

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(ZookeeperShard, TZookeeperShard);

    struct TCreateZookeeperShardOptions
    {
        //! Hint for shard object id. Null means no hint.
        TZookeeperShardId HintId;

        //! Name of the shard.
        TString Name;

        //! Path of the shard root.
        TZookeeperPath RootPath;

        //! Cell tag shard lives on.
        NObjectClient::TCellTag CellTag;
    };
    virtual TZookeeperShard* CreateZookeeperShard(const TCreateZookeeperShardOptions& options) = 0;

    virtual void ZombifyZookeeperShard(TZookeeperShard* shard) = 0;

    //! Returns zookeeper shard with given name or |nullptr| if such shard does not exist.
    virtual TZookeeperShard* FindZookeeperShardByName(const TString& name) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IZookeeperManager)

////////////////////////////////////////////////////////////////////////////////

IZookeeperManagerPtr CreateZookeeperManager(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
