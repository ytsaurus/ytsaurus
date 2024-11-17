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
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        const auto& shards = zookeeperManager->ZookeeperShards();

        std::vector<std::string> keys;
        keys.reserve(std::min(limit, std::ssize(shards)));
        for (auto [shardId, shard] : shards) {
            if (std::ssize(keys) >= limit) {
                break;
            }
            keys.push_back(shard->GetName());
        }

        return keys;
    }

    i64 GetSize() const override
    {
        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        return zookeeperManager->ZookeeperShards().size();
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& zookeeperManager = Bootstrap_->GetZookeeperManager();
        auto* shard = zookeeperManager->FindZookeeperShardByName(key);
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
