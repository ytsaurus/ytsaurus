#include "shard_map_type_handler.h"
#include "cypress_manager.h"
#include "shard.h"
#include "virtual.h"

#include <yt/server/master/cell_master/bootstrap.h>

#include <yt/core/ytree/virtual.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualShardMap
    : public TVirtualMulticellMapBase
{
public:
    TVirtualShardMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMulticellMapBase(bootstrap, owningNode)
    { }

private:
    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return ToObjectIds(GetValues(cypressManager->Shards(), sizeLimit));
    }

    virtual bool IsValid(TObject* object) const override
    {
        return object->GetType() == EObjectType::CypressShard;
    }

    virtual i64 GetSize() const override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->Shards().GetSize();
    }

    virtual NYPath::TYPath GetWellKnownPath() const override
    {
        return "//sys/cypress_shards";
    }
};

INodeTypeHandlerPtr CreateShardMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::CypressShardMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualShardMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
