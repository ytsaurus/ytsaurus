#include "shard_map_type_handler.h"
#include "cypress_manager.h"
#include "shard.h"
#include "virtual.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/core/ytree/virtual.h>

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
    using TVirtualMulticellMapBase::TVirtualMulticellMapBase;

private:
    TFuture<std::vector<TObjectId>> GetKeys(i64 sizeLimit) const override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return MakeFuture(ToObjectIds(GetValues(cypressManager->Shards(), sizeLimit)));
    }

    bool IsValid(TObject* object) const override
    {
        return object->GetType() == EObjectType::CypressShard;
    }

    TFuture<i64> GetSize() const override
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return MakeFuture<i64>(cypressManager->Shards().GetSize());
    }

    NYPath::TYPath GetWellKnownPath() const override
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
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualShardMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
