#include "stdafx.h"
#include "cypress_integration.h"
#include "account.h"
#include "user.h"
#include "group.h"

#include <core/misc/collection_helpers.h>

#include <core/ytree/virtual.h>

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
    for (const auto* object : objects) {
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
        return ToNames(GetValues(securityManager->Accounts(), sizeLimit));
    }

    virtual size_t GetSize() const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return securityManager->Accounts().GetSize();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* account = securityManager->FindAccountByName(Stroka(key));
        if (!IsObjectAlive(account)) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(account);
    }
};

INodeTypeHandlerPtr CreateAccountMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualAccountMap>(bootstrap);
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::AccountMap,
        service,
        EVirtualNodeOptions(EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf));
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
        return ToNames(GetValues(securityManager->Users(), sizeLimit));
    }

    virtual size_t GetSize() const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return securityManager->Users().GetSize();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* user = securityManager->FindUserByName(Stroka(key));
        if (!IsObjectAlive(user)) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(user);
    }
};

INodeTypeHandlerPtr CreateUserMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualUserMap>(bootstrap);
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::UserMap,
        service,
        EVirtualNodeOptions(EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf));
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
        return ToNames(GetValues(securityManager->Groups(), sizeLimit));
    }

    virtual size_t GetSize() const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        return securityManager->Groups().GetSize();
    }

    virtual IYPathServicePtr FindItemService(const TStringBuf& key) const override
    {
        auto securityManager = Bootstrap->GetSecurityManager();
        auto* group = securityManager->FindGroupByName(Stroka(key));
        if (!IsObjectAlive(group)) {
            return nullptr;
        }

        auto objectManager = Bootstrap->GetObjectManager();
        return objectManager->GetProxy(group);
    }
};

INodeTypeHandlerPtr CreateGroupMapTypeHandler(TBootstrap* bootstrap)
{
    YCHECK(bootstrap);

    auto service = New<TVirtualGroupMap>(bootstrap);
    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::GroupMap,
        service,
        EVirtualNodeOptions(EVirtualNodeOptions::RequireLeader | EVirtualNodeOptions::RedirectSelf));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
