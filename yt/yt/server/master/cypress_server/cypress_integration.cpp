#include "cypress_integration.h"
#include "cypress_manager.h"
#include "virtual.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/client/object_client/helpers.h>

namespace NYT::NCypressServer {

using namespace NYTree;
using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

class TVirtualLockMap
    : public TVirtualMulticellMapBase
{
public:
    TVirtualLockMap(
        TBootstrap* bootstrap,
        INodePtr owningNode)
        : TVirtualMulticellMapBase(bootstrap, owningNode)
    { }

private:
    virtual std::vector<TObjectId> GetKeys(i64 sizeLimit) const override
    {
        return NYT::GetKeys(Locks(), sizeLimit);
    }

    virtual bool IsValid(TObject* object) const override
    {
        return IsObjectAlive(object);
    }

    virtual bool NeedSuppressUpstreamSync() const override
    {
        return false;
    }

    virtual i64 GetSize() const override
    {
        return Locks().size();
    }

    virtual NYPath::TYPath GetWellKnownPath() const override
    {
        return "//sys/locks";
    }

    const TReadOnlyEntityMap<TLock>& Locks() const
    {
        const auto& cypressManager = Bootstrap_->GetCypressManager();
        return cypressManager->Locks();
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateLockMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::LockMap,
        BIND([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualLockMap>(bootstrap, std::move(owningNode));
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
