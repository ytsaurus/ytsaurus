#include "zookeeper_shard.h"

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NZookeeperMaster {

////////////////////////////////////////////////////////////////////////////////

TZookeeperShard::TZookeeperShard(TZookeeperShardId id)
    : ShardId_(id)
{ }

void TZookeeperShard::Save(TStreamSaveContext& context) const
{
    using NYT::Save;

    Save(context, ShardId_);
    Save(context, Name_);
    Save(context, RootPath_);
}

void TZookeeperShard::Load(TStreamLoadContext& context)
{
    using NYT::Load;

    Load(context, ShardId_);
    Load(context, Name_);
    Load(context, RootPath_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperMaster
