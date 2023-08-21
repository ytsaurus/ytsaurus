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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TVirtualAccountMap
    : public TVirtualMapBase
{
public:
    TVirtualAccountMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    {
        SetOpaque(true);
    }

private:
    using TBase = TVirtualMapBase;

    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        std::vector<TString> names;
        names.reserve(std::min<size_t>(securityManager->Accounts().size(), sizeLimit));

        for (auto [accountId, account] : securityManager->Accounts()) {
            if (std::ssize(names) >= sizeLimit) {
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

    IYPathServicePtr FindItemService(TStringBuf key) const override
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
                SerializeClusterResourceLimits(resources, consumer, Bootstrap_, /* serializeDiskSpace */ true);
                return true;
            }

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
    : public TVirtualMapBase
{
public:
    TVirtualAccountResourceUsageLeaseMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();

        std::vector<TString> keys;
        keys.reserve(sizeLimit);
        for (auto [accountResourceUsageLeaseId, _] : securityManager->AccountResourceUsageLeases()) {
            if (static_cast<i64>(keys.size()) >= sizeLimit) {
                break;
            }
            keys.emplace_back(ToString(accountResourceUsageLeaseId));
        }
        return keys;
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->AccountResourceUsageLeases().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
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
    : public TVirtualMapBase
{
public:
    TVirtualUserMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return ToNames(GetValues(securityManager->Users(), sizeLimit));
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->Users().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
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
    : public TVirtualMapBase
{
public:
    TVirtualGroupMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return ToNames(GetValues(securityManager->Groups(), sizeLimit));
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->Groups().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
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
    : public TVirtualMapBase
{
public:
    TVirtualNetworkProjectMap(TBootstrap* bootstrap, INodePtr owningNode)
        : TVirtualMapBase(owningNode)
        , Bootstrap_(bootstrap)
    { }

private:
    TBootstrap* const Bootstrap_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return ToNames(GetValues(securityManager->NetworkProjects(), sizeLimit));
    }

    i64 GetSize() const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->NetworkProjects().GetSize();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto* networkProject = securityManager->FindNetworkProjectByName(static_cast<TString>(key));
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
    : public TVirtualMapBase
{
public:
    TVirtualProxyRoleMap(TBootstrap* bootstrap, INodePtr owningNode, EObjectType type)
        : TVirtualMapBase(std::move(owningNode))
        , Bootstrap_(bootstrap)
        , Type_(type)
    { }

private:
    TBootstrap* const Bootstrap_;

    const EObjectType Type_;

    std::vector<TString> GetKeys(i64 sizeLimit) const override
    {
        return ToNames(GetValues(GetProxyRoles(), sizeLimit));
    }

    i64 GetSize() const override
    {
        return GetProxyRoles().size();
    }

    IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        const auto& proxyRoles = GetProxyRoles();
        auto proxyRolesIt = proxyRoles.find(static_cast<TString>(key));
        if (proxyRolesIt == proxyRoles.end()) {
            return nullptr;
        }

        auto* proxyRole = proxyRolesIt->second;
        if (!IsObjectAlive(proxyRole)) {
            return nullptr;
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        return objectManager->GetProxy(proxyRole);
    }

    const THashMap<TString, TProxyRole*>& GetProxyRoles() const
    {
        EProxyKind proxyKind;
        switch (Type_) {
            case EObjectType::HttpProxyRoleMap:
                proxyKind = EProxyKind::Http;
                break;
            case EObjectType::RpcProxyRoleMap:
                proxyKind = EProxyKind::Rpc;
                break;
            default:
                YT_ABORT();
        }

        const auto& securityManager = Bootstrap_->GetSecurityManager();
        return securityManager->GetProxyRolesWithProxyKind(proxyKind);
    }
};

INodeTypeHandlerPtr CreateProxyRoleMapTypeHandler(TBootstrap* bootstrap, EObjectType type)
{
    YT_VERIFY(bootstrap);

    return CreateVirtualTypeHandler(
        bootstrap,
        type,
        BIND_NO_PROPAGATE([=] (INodePtr owningNode) -> IYPathServicePtr {
            return New<TVirtualProxyRoleMap>(bootstrap, owningNode, type);
        }),
        EVirtualNodeOptions::RedirectSelf);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
