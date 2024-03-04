#include "cypress_integration.h"

#include "zookeeper_manager.h"
#include "zookeeper_shard.h"

#include <yt/yt/server/master/cypress_server/virtual.h>

namespace NYT::NZookeeperServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualZookeeperShardMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<TString> GetKeys(i64 /*sizeLimit*/) const override
    {
        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        const auto& shards = zookeeperManager->ZookeeperShards();

        std::vector<TString> keys;
        keys.reserve(shards.size());
        for (auto [shardId, shard] : shards) {
            keys.push_back(shard->GetName());
        }

        return keys;
    }

    i64 GetSize() const override
    {
        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        return zookeeperManager->ZookeeperShards().size();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        auto* shard = zookeeperManager->FindZookeeperShardByName(TString(key));
        if (!IsObjectAlive(shard)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(shard);
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateZookeeperShardMapTypeHandler(TBootstrap* bootstrap)
{
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::ZookeeperShardMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualZookeeperShardMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
