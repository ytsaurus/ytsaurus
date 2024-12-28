#include "private.h"
#include "cypress_integration.h"
#include "account.h"
#include "account_resource_usage_lease.h"
#include "group.h"
#include "helpers.h"
#include "user.h"
#include "network_project.h"
#include "proxy_role.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/cypress_server/virtual.h>

#include <yt/yt/server/master/object_server/object_detail.h>
#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/object_server/helpers.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NSecurityServer {

using namespace NYTree;
using namespace NCypressServer;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NSecurityClient;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

EObjectType RoleMapObjectTypeFromProxyKind(EProxyKind proxyKind)
{
    switch (proxyKind) {
        case EProxyKind::Http:
            return EObjectType::HttpProxyRoleMap;
        case EProxyKind::Rpc:
            return EObjectType::RpcProxyRoleMap;
        default:
            YT_ABORT();
    }
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TVirtualAccountMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    using TBase = TVirtualMapBase;

    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        std::vector<std::string> names;
        names.reserve(std::min(limit, std::ssize(securityManager->Accounts())));

        for (auto [accountId, account] : securityManager->Accounts()) {
            if (std::ssize(names) >= limit) {
                break;
            }
            if (!account->GetParent() && account != securityManager->GetRootAccount() && IsObjectAlive(account)) {
                YT_LOG_ALERT("Unattended account (Id: %v)",
                    account->GetId());
            }
            names.push_back(account->GetName());
        }

        return names;
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->Accounts().GetSize();
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* account = securityManager->FindAccountByName(TString(key), false /*activeLifeStageOnly*/);
        if (!account) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(account);
    }

    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TotalResourceUsage));
        descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::TotalCommittedResourceUsage));
        descriptors->push_back(EInternedAttributeKey::TotalResourceLimits);
        descriptors->push_back(EInternedAttributeKey::RootAccountResourceLimits);
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* rootAccount = securityManager->GetRootAccount();

        switch (key) {
            case EInternedAttributeKey::TotalResourceUsage:
            case EInternedAttributeKey::TotalCommittedResourceUsage: {
                SerializeAccountClusterResourceUsage(
                    rootAccount,
                    key == EInternedAttributeKey::CommittedResourceUsage,
                    /*recursive*/ true,
                    consumer,
                    Bootstrap_);
                return true;
            }

            case EInternedAttributeKey::TotalResourceLimits: {
                auto resources = rootAccount->ComputeTotalChildrenLimits();
                SerializeClusterResourceLimits(resources, consumer, Bootstrap_, /*serializeDiskSpace*/ true);
                return true;
            }

            case EInternedAttributeKey::RootAccountResourceLimits:
                SerializeClusterResourceLimits(
                    rootAccount->ClusterResourceLimits(),
                    consumer,
                    Bootstrap_,
                    /*serializeDiskSpace*/ true);
                return true;

            default:
                return TBase::GetBuiltinAttribute(key, consumer);
        }
    }
};

INodeTypeHandlerPtr CreateAccountMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::AccountMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualAccountMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualAccountResourceUsageLeaseMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        std::vector<std::string> keys;
        keys.reserve(std::min(limit, securityManager->AccountResourceUsageLeases().GetSize()));
        for (auto [accountResourceUsageLeaseId, _] : securityManager->AccountResourceUsageLeases()) {
            if (std::ssize(keys) >= limit) {
                break;
            }
            keys.push_back(ToString(accountResourceUsageLeaseId));
        }
        return keys;
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->AccountResourceUsageLeases().GetSize();
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto accountResourceUsageLeaseId = TAccountResourceUsageLeaseId::FromString(key);
        auto* accountResourceUsageLease = securityManager->FindAccountResourceUsageLease(accountResourceUsageLeaseId);
        if (!accountResourceUsageLease) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(accountResourceUsageLease);
    }
};

INodeTypeHandlerPtr CreateAccountResourceUsageLeaseMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::AccountResourceUsageLeaseMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualAccountResourceUsageLeaseMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualUserMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return ToNames(GetValues(securityManager->Users(), limit));
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->Users().GetSize();
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* user = securityManager->FindUserByNameOrAlias(TString(key), false /*activeLifeStageOnly*/);
        if (!IsObjectAlive(user)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(user);
    }
};

INodeTypeHandlerPtr CreateUserMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::UserMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualUserMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualGroupMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return ToNames(GetValues(securityManager->Groups(), limit));
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->Groups().GetSize();
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* group = securityManager->FindGroupByNameOrAlias(TString(key));
        if (!IsObjectAlive(group)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(group);
    }
};

INodeTypeHandlerPtr CreateGroupMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::GroupMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualGroupMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualNetworkProjectMap
    : public TVirtualSinglecellMapBase
{
public:
    using TVirtualSinglecellMapBase::TVirtualSinglecellMapBase;

private:
    std::vector<std::string> GetKeys(i64 limit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return ToNames(GetValues(securityManager->NetworkProjects(), limit));
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->NetworkProjects().GetSize();
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* networkProject = securityManager->FindNetworkProjectByName(key);
        if (!IsObjectAlive(networkProject)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(networkProject);
    }
};

INodeTypeHandlerPtr CreateNetworkProjectMapTypeHandler(TBootstrap* bootstrap)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        EObjectType::NetworkProjectMap,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualNetworkProjectMap>(bootstrap, owningNode);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

class TVirtualProxyRoleMap
    : public TVirtualSinglecellMapBase
{
public:
    TVirtualProxyRoleMap(TBootstrap* bootstrap, INodePtr owningNode, EProxyKind proxyKind)
        : TVirtualSinglecellMapBase(bootstrap, std::move(owningNode))
        , ProxyKind_(proxyKind)
    { }

private:
    const EProxyKind ProxyKind_;

    std::vector<std::string> GetKeys(i64 limit) const override
    {
        return ToNames(GetValues(GetProxyRoles(), limit));
    }

    i64 GetSize() const override
    {
        return std::ssize(GetProxyRoles());
    }

    IYPathServicePtr FindItemService(const std::string& key) const override
    {
        const auto& proxyRoles = GetProxyRoles();
        auto it = proxyRoles.find(key);
        if (it == proxyRoles.end()) {
            return nullptr;
        }

        auto* proxyRole = it->second;
        if (!IsObjectAlive(proxyRole)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(proxyRole);
    }

    const THashMap<std::string, TProxyRole*>& GetProxyRoles() const
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->GetProxyRolesWithProxyKind(ProxyKind_);
    }
};

////////////////////////////////////////////////////////////////////////////////

INodeTypeHandlerPtr CreateProxyRoleMapTypeHandler(TBootstrap* bootstrap, EProxyKind proxyKind)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        RoleMapObjectTypeFromProxyKind(proxyKind),
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualProxyRoleMap>(bootstrap, owningNode, proxyKind);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
