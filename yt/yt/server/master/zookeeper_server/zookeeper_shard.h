#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/zookeeper_master/zookeeper_shard.h>

namespace NYT::NZookeeperServer {

////////////////////////////////////////////////////////////////////////////////

//! Master object corresponding to zookeeper shard.
class TZookeeperShard
    : public NObjectServer::TObject
    , public NZookeeperMaster::TZookeeperShard
    , public TRefTracked<TZookeeperShard>
{
public:
    //! Tag of the master cell shard lives in.
    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TCellTag, CellTag);

public:
    explicit TZookeeperShard(TZookeeperShardId id);

    // Logging stuff.
    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    // Persistence.
    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TZookeeperShard)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
