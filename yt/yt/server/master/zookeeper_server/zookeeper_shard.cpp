#include "zookeeper_shard.h"

#include <yt/yt/server/master/cell_master/serialize.h>

namespace NYT::NZookeeperServer {

using namespace NCellMaster;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TZookeeperShard::TZookeeperShard(TZookeeperShardId id)
    : TObject(id)
    , NZookeeperMaster::TZookeeperShard(id)
{ }

TString TZookeeperShard::GetLowercaseObjectName() const
{
    return Format("zookeeper shard %v", GetId());
}

TString TZookeeperShard::GetCapitalizedObjectName() const
{
    return Format("Zookeeper shard %v", GetId());
}

TString TZookeeperShard::GetObjectPath() const
{
    return Format("//sys/zookeeper_shards/%v", GetId());
}

void TZookeeperShard::Save(TSaveContext& context) const
{
    using NYT::Save;

    TObject::Save(context);
    NZookeeperMaster::TZookeeperShard::Save(context);

    Save(context, CellTag_);
}

void TZookeeperShard::Load(TLoadContext& context)
{
    using NYT::Load;

    TObject::Load(context);
    NZookeeperMaster::TZookeeperShard::Load(context);

    Load(context, CellTag_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
