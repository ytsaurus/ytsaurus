#include "cypress_integration.h"
#include "scheduler_pool_manager.h"

#include <yt/yt/server/lib/object_server/helpers.h>

#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NSchedulerPoolServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualPoolTreeMap
    : public TVirtualSinglecellMapBase
{
public:
    TVirtualPoolTreeMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualSinglecellMapBase(bootstrap, std::move(owningNode))
    {
        SetOpaque(false);
    }

private:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        std::vector<std::string> names;
        names.reserve(std::min(limit, GetSize()));
        for (const auto& [_, poolTree] : GetPoolTrees()) {
            if (std::ssize(names) >= limit) {
                break;
            }
            names.push_back(poolTree->GetTreeName());
        }
        return names;
    }

    i64 GetSize() const override
    {
        return std::ssize(GetPoolTrees());
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        auto* poolTree = Bootstrap_->GetSchedulerPoolManager()->FindPoolTreeObjectByName(key);
        if (!IsObjectAlive(poolTree)) {
            return nullptr;
        }
        return Bootstrap_->GetObjectManager()->GetProxy(poolTree);
    }

    const THashMap<std::string, TSchedulerPoolTree*>& GetPoolTrees() const
    {
        return Bootstrap_->GetSchedulerPoolManager()->GetPoolTrees();
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreatePoolTreeMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::SchedulerPoolTreeMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualPoolTreeMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
