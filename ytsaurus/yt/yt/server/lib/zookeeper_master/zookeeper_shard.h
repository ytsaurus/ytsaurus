#pragma once

#include "public.h"

#include <library/cpp/yt/misc/property.h>

namespace NYT::NZookeeperMaster {

////////////////////////////////////////////////////////////////////////////////

class TZookeeperShard
{
public:
    //! Unique identifier of the shard.
    DEFINE_BYVAL_RO_PROPERTY(TZookeeperShardId, ShardId);

    //! Name of the shard.
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

    //! Path of the shard root.
    DEFINE_BYVAL_RW_PROPERTY(TZookeeperPath, RootPath);

public:
    explicit TZookeeperShard(TZookeeperShardId id);

    // Persistence.
    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperMaster
