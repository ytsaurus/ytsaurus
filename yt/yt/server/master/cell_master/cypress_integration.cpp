#include "cypress_integration.h"

#include "bootstrap.h"
#include "master_cell_group.h"
#include "master_cell_group_manager.h"

#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NCellMaster {

using namespace NCypressServer;
using namespace NObjectServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TVirtualMasterCellGroupMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& groupManager = Bootstrap_->GetMasterCellGroupManager();
        const auto& cellGroups = groupManager->MasterCellGroups();
        std::vector<std::string> names;
        names.reserve(std::min(limit, cellGroups.GetSize()));
        for (auto* group : GetValues(cellGroups, limit)) {
            if (!IsObjectAlive(group)) {
                continue;
            }
            names.push_back(group->GetName());
        }
        return names;
    }

    i64 GetSize() const override
    {
        const auto& groupManager = Bootstrap_->GetMasterCellGroupManager();
        int size = 0;
        for (auto* group : GetValues(groupManager->MasterCellGroups())) {
            if (IsObjectAlive(group)) {
                ++size;
            }
        }
        return size;
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& groupManager = Bootstrap_->GetMasterCellGroupManager();
        auto* group = groupManager->FindMasterCellGroupByName(key);
        if (!IsObjectAlive(group)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(group);
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateMasterCellGroupMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::MasterCellGroupMap,
        BIND_NO_PROPAGATE([=] (INodePtr /*owningNode*/) -> IYPathServicePtr {
            return New<TVirtualMasterCellGroupMap>(bootstrap);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
