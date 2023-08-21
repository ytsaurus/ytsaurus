#include "zookeeper_shard_type_handler.h"

#include "zookeeper_manager.h"
#include "zookeeper_shard.h"
#include "zookeeper_shard_proxy.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

namespace NYT::NZookeeperServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TZookeeperShardTypeHandler
    : public TObjectTypeHandlerWithMapBase<TZookeeperShard>
{
public:
    TZookeeperShardTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TZookeeperShard>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
    { }

    ETypeFlags GetFlags() const override
    {
        return
            ETypeFlags::ReplicateCreate |
            ETypeFlags::ReplicateDestroy |
            ETypeFlags::ReplicateAttributes |
            ETypeFlags::Creatable |
            ETypeFlags::Removable;
    }

    EObjectType GetType() const override
    {
        return EObjectType::ZookeeperShard;
    }

    TObject* CreateObject(
        TZookeeperShardId hintId,
        IAttributeDictionary* attributes) override
    {
        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        return zookeeperManager->CreateZookeeperShard({
            .HintId = hintId,
            .Name = attributes->GetAndRemove<TString>("name"),
            .RootPath = attributes->GetAndRemove<TZookeeperPath>("root_path"),
            .CellTag = attributes->GetAndRemove<TCellTag>("cell_tag", Bootstrap_->GetPrimaryCellTag()),
        });
    }

private:
    TCellTagList DoGetReplicationCellTags(const TZookeeperShard* shard) override
    {
        if (shard->GetCellTag() == Bootstrap_->GetPrimaryCellTag()) {
            return {};
        } else {
            return {shard->GetCellTag()};
        }
    }

    IObjectProxyPtr DoGetProxy(TZookeeperShard* shard, TTransaction* /*transaction*/) override
    {
        return CreateZookeeperShardProxy(Bootstrap_, &Metadata_, shard);
    }

    void DoZombifyObject(TZookeeperShard* shard) override
    {
        TObjectTypeHandlerWithMapBase::DoZombifyObject(shard);

        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        zookeeperManager->ZombifyZookeeperShard(shard);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateZookeeperShardTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TZookeeperShard>* map)
{
    return New<TZookeeperShardTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
