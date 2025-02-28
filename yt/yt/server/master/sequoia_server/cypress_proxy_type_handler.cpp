#include "cypress_proxy_type_handler.h"

#include "cypress_proxy_object.h"
#include "cypress_proxy_object_proxy.h"
#include "cypress_proxy_tracker.h"

#include <yt/yt/server/master/object_server/type_handler_detail.h>

#include <yt/yt/server/master/transaction_server/public.h>

namespace NYT::NSequoiaServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectServer;
using namespace NTransactionServer;

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyTypeHandler
    : public TObjectTypeHandlerWithMapBase<TCypressProxyObject>
{
public:
    TCypressProxyTypeHandler(
        TBootstrap* bootstrap,
        TEntityMap<TCypressProxyObject>* map)
        : TObjectTypeHandlerWithMapBase(bootstrap, map)
    { }

    ETypeFlags GetFlags() const override
    {
        return ETypeFlags::Removable;
    }

    EObjectType GetType() const override
    {
        return EObjectType::CypressProxyObject;
    }

private:
    IObjectProxyPtr DoGetProxy(TCypressProxyObject* proxyObject, TTransaction* /*transaction*/) override
    {
        return CreateCypressProxyObjectProxy(Bootstrap_, &Metadata_, proxyObject);
    }

    void DoZombifyObject(TCypressProxyObject* proxyObject) noexcept override
    {
        const auto& cypressProxyTracker = Bootstrap_->GetCypressProxyTracker();
        cypressProxyTracker->ZombifyCypressProxy(proxyObject);

        TObjectTypeHandlerWithMapBase::DoZombifyObject(proxyObject);
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectTypeHandlerPtr CreateCypressProxyTypeHandler(
    TBootstrap* bootstrap,
    TEntityMap<TCypressProxyObject>* map)
{
    return New<TCypressProxyTypeHandler>(bootstrap, map);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaServer
