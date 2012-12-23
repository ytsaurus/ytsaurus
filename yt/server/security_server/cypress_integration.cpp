#include "stdafx.h"
#include "cypress_integration.h"
#include "account.h"

#include <ytlib/ytree/virtual.h>

#include <server/cypress_server/virtual.h>

#include <server/cell_master/bootstrap.h>

#include <server/security_server/security_manager.h>

#include <server/object_server/object_manager.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TVirtualAccountMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualAccountMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto accounts = securityManager->GetAccounts(sizeLimit);

        std::vector<Stroka> result;
        result.reserve(accounts.size());

        FOREACH (auto* account, accounts) {
            result.push_back(account->GetName());
        }
        return result;
    }

    virtual size_t GetSize() const
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return securityManager->GetAccountCount();
    }

    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* account = securityManager->FindAccountByName(Stroka(key));
        if (!account) {
            return NULL;
        }
        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->FindProxy(account->GetId());
    }
};

INodeTypeHandlerPtr CreateAccountMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::AccountMap,
        New<TVirtualAccountMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
