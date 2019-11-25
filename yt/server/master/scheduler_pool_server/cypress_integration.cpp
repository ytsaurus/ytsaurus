#include "cypress_integration.h"
#include "scheduler_pool_manager.h"

#include <yt/server/lib/misc/object_helpers.h>

#include <yt/server/master/cypress_server/virtual.h>

#include <yt/core/ytree/public.h>

namespace NYT::NSchedulerPoolServer {

using namespace NCellMaster;
using namespace NCypressServer;
using namespace NObjectClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualPoolTreeMap
    : public TVirtualMapBase
{
public:
    TVirtualPoolTreeMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(std::move(owningNode))
        , Bootstrap_(bootstrap)
    {
        SetOpaque(false);
    }

private:
    const TBootstrap* Bootstrap_;

    virtual std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        sizeLimit = std::min<i64>(GetSize(), sizeLimit);
        std::vector<TString> names;
        names.reserve(sizeLimit);
        for (const auto& [_, poolTree] : GetPoolTrees()) {
            names.push_back(poolTree->GetTreeName());
            if (names.size() == sizeLimit) {
                break;
            }
        }
        return names;
    }

    virtual i64 GetSize() const override
    {
        return GetPoolTrees().size();
    }

    virtual IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto* poolTree = Bootstrap_->GetSchedulerPoolManager()->FindPoolTreeObjectByName(TString(key));
        if (!IsObjectAlive(poolTree)) {
            return nullptr;
        }
        return Bootstrap_->GetObjectManager()->GetProxy(poolTree);
    }

    const THashMap<TString, TSchedulerPoolTree*>& GetPoolTrees() const
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
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualPoolTreeMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSchedulerPoolServer
