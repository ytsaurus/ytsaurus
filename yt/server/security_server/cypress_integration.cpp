#include "stdafx.h"
#include "cypress_integration.h"
#include "account.h"
#include "user.h"
#include "group.h"

#include <ytlib/ytree/virtual.h>

#include <server/cypress_server/virtual.h>

#include <server/cell_master/bootstrap.h>

#include <server/security_server/security_manager.h>

#include <server/object_server/object_manager.h>
#include <server/object_server/object_detail.h>

namespace NYT {
namespace NSecurityServer {

using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class T>
std::vector<Stroka> ToNames(const std::vector<T>& objects)
{
    std::vector<Stroka> names;
    names.reserve(objects.size());
    FOREACH (auto* object, objects) {
        names.push_back(object->GetName());
    }
    return names;
}

} // namespace

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

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return ToNames(securityManager->GetAccounts(sizeLimit));
    }

    virtual size_t GetSize() const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return securityManager->GetAccountCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* account = securityManager->FindAccountByName(Stroka(key));
        if (!account || !account->IsAlive()) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(account);
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

class TVirtualUserMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualUserMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return ToNames(securityManager->GetUsers(sizeLimit));
    }

    virtual size_t GetSize() const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return securityManager->GetUserCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* user = securityManager->FindUserByName(Stroka(key));
        if (!user || !user->IsAlive()) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(user);
    }
};

INodeTypeHandlerPtr CreateUserMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::UserMap,
        New<TVirtualUserMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualGroupMap
    : public TVirtualMapBase
{
public:
    explicit TVirtualGroupMap(TBootstrap* bootstrap)
        : Bootstrap(bootstrap)
    { }

private:
    TBootstrap* Bootstrap;

    virtual std::vector<Stroka> GetKeys(size_t sizeLimit) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return ToNames(securityManager->GetGroups(sizeLimit));
    }

    virtual size_t GetSize() const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return securityManager->GetGroupCount();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* group = securityManager->FindGroupByName(Stroka(key));
        if (!group || !group->IsAlive()) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(group);
    }
};

INodeTypeHandlerPtr CreateGroupMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::GroupMap,
        New<TVirtualGroupMap>(bootstrap));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
