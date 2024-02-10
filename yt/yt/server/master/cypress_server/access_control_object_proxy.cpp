#include "access_control_object_proxy.h"
#include "access_control_object.h"
#include "access_control_object_namespace.h"
#include "cypress_manager.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>

#include <yt/yt/server/master/object_server/object_detail.h>
#include <yt/yt/server/master/object_server/type_handler.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/node_detail.h>
#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NTransactionServer;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TAccessControlObjectProxy
    : public TNonversionedObjectProxyBase<TAccessControlObject>
    , public virtual TNodeBase
    , public virtual TMapNodeMixin
    , public THierarchicPermissionValidator<TObject>
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Map)

private:
    using TBase = TNonversionedObjectProxyBase<TAccessControlObject>;

public:
    TAccessControlObjectProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TObject* object)
        : TBase(bootstrap, metadata, object)
        , THierarchicPermissionValidator<TObject>(CreatePermissionValidator())
    { }

    TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const override
    {
        return this;
    }

    TIntrusivePtr<NYTree::ICompositeNode> AsComposite() override
    {
        return this;
    }

    std::unique_ptr<ITransactionalNodeFactory> CreateFactory() const override
    {
        THROW_ERROR_EXCEPTION("CreateFactory() method is not supported for access control objects");
    }

    ICompositeNodePtr GetParent() const override
    {
        const auto* thisImpl = GetThisImpl();
        const auto& parent = thisImpl->Namespace();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& parentHandler = objectManager->GetHandler(parent.Get());
        return DynamicPointerCast<ICompositeNode>(
            parentHandler->GetProxy(parent.Get(), /*transaction*/ nullptr));
    }

    void SetParent(const ICompositeNodePtr& /*parent*/) override
    {
        YT_ABORT();
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        // Resolve ambiguity between the two base class overrides.
        return TBase::DoInvoke(context);
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        TNodeBase::GetSelf(request, response, context);
    }

    TResolveResult ResolveRecursive(const NYPath::TYPath& path, const IYPathServiceContextPtr& context) override
    {
        return TMapNodeMixin::ResolveRecursive(path, context);
    }

    void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override
    {
        TNodeBase::RemoveSelf(request, response, context);
    }

    void DoRemoveSelf(bool /*recursive*/, bool /*force*/) override
    {
        auto* thisImpl = GetThisImpl();
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RemoveObject(thisImpl);
    }

    int GetChildCount() const override
    {
        return 1;
    }

    void ReplaceChild(const INodePtr& /*oldChild*/, const INodePtr& /*newChild*/) override
    {
        Y_UNREACHABLE();
    }

    void RemoveChild(const INodePtr& /*child*/) override
    {
        THROW_ERROR_EXCEPTION("Access control object child cannot be removed");
    }

    bool RemoveChild(const TString& key) override
    {
        auto child = FindChild(key);

        if (!child) {
            return false;
        }

        RemoveChild(child);

        SetModified(EModificationType::Content);

        return true;
    }

    std::vector<std::pair<TString, INodePtr>> GetChildren() const override
    {
        auto childNode = FindChild(ChildKey);
        return {{ChildKey, std::move(childNode)}};
    }

    std::vector<TString> GetKeys() const override
    {
        return {ChildKey};
    }

    bool AddChild(const TString& /*key*/, const INodePtr& /*child*/) override
    {
        THROW_ERROR_EXCEPTION("Access control object children are pre-defined and cannot be added directly");
    }

    std::optional<TString> FindChildKey(const IConstNodePtr& /*child*/) override
    {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }

    INodePtr FindChild(const TString& key) const override;

    void Clear() override
    {
        SetModified(EModificationType::Content);
    }

private:
    friend class TAccessControlObjectNamespaceProxy;
    friend class TAccessControlPrincipalProxy;

    static const inline TString ChildKey{"principal"};

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetReplicated(true)
            .SetMandatory(true));
        descriptors->push_back(ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Namespace)
            .SetReplicated(true)
            .SetMandatory(true));

        descriptors->push_back(ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::PrincipalAcl)
            .SetWritable(true)
            .SetReplicated(true)
            .SetWritePermission(EPermission::Administer));
        descriptors->push_back(ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::PrincipalOwner)
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true)
            .SetPresent(GetThisImpl()->PrincipalAcd().GetOwner()));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* thisImpl = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(thisImpl->GetName());
                return true;

            case EInternedAttributeKey::Namespace:
                BuildYsonFluently(consumer)
                    .Value(thisImpl->Namespace()->GetName());
                return true;

            case EInternedAttributeKey::PrincipalAcl:
                BuildYsonFluently(consumer)
                    .Value(thisImpl->PrincipalAcd().Acl());
                return true;

            case EInternedAttributeKey::PrincipalOwner: {
                const auto& principalAcd = thisImpl->PrincipalAcd();
                if (!principalAcd.GetOwner()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(principalAcd.GetOwner()->GetName());
                return true;
            }

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        const auto& securityManager = Bootstrap_->GetSecurityManager();
        auto& principalAcd = GetThisImpl()->PrincipalAcd();

        switch (key) {
            case EInternedAttributeKey::PrincipalAcl: {
                auto newAcl = DeserializeAclOrThrow(
                    ConvertToNode(value),
                    securityManager);
                principalAcd.SetEntries(newAcl);

                SetModified(EModificationType::Attributes);

                return true;
            }

            case EInternedAttributeKey::PrincipalOwner: {
                auto name = ConvertTo<TString>(value);
                auto* owner = securityManager->GetSubjectByNameOrAliasOrThrow(name, true /*activeLifeStageOnly*/);
                auto* user = securityManager->GetAuthenticatedUser();
                if (user != owner && !securityManager->IsSuperuser(user)) {
                    THROW_ERROR_EXCEPTION(
                        NSecurityClient::EErrorCode::AuthorizationError,
                        "Access denied: can only set owner to self");
                }
                principalAcd.SetOwner(owner);

                SetModified(EModificationType::Attributes);

                return true;
            }

            default:
                break;
        }

        return TBase::SetBuiltinAttribute(key, value, force);
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        switch (key) {
            case EInternedAttributeKey::PrincipalOwner: {
                auto& principalAcd = GetThisImpl()->PrincipalAcd();
                if (principalAcd.GetOwner()) {
                    principalAcd.SetOwner(nullptr);
                }

                SetModified(EModificationType::Attributes);

                return true;
            }

            default:
                break;
        }

        return false;
    }

    void ValidatePermission(EPermissionCheckScope scope, EPermission permission, const TString& /*user*/) override
    {
        THierarchicPermissionValidator::ValidatePermission(GetThisImpl(), scope, permission);
    }

    TCompactVector<TObject*, 1> ListDescendantsForPermissionValidation(TObject* object) override
    {
        YT_ASSERT(dynamic_cast<TAccessControlObject*>(object));

        return {};
    }

    TObject* GetParentForPermissionValidation(TObject* object) override
    {
        YT_ASSERT(dynamic_cast<TAccessControlObject*>(object));

        auto* accessControlObject = static_cast<TAccessControlObject*>(object);
        return accessControlObject->Namespace().Get();
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateAccessControlObjectProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TAccessControlObject* accessControlObject)
{
    return New<TAccessControlObjectProxy>(bootstrap, metadata, accessControlObject);
}

///////////////////////////////////////////////////////////////////////////////

class TAccessControlPrincipalProxy
    : public virtual TNodeBase
    , public virtual TSupportsAttributes
    , public ISystemAttributeProvider
{
public:
    TAccessControlPrincipalProxy(
        TBootstrap* bootstrap,
        TEphemeralObjectPtr<TAccessControlObject> owner)
        : Bootstrap_(bootstrap)
        , Owner_(std::move(owner))
    { }

protected:
    using TBase = TNodeBase;
    using TSelf = TAccessControlPrincipalProxy;

    DECLARE_YPATH_SERVICE_METHOD(NObjectClient::NProto, CheckPermission);

    const IAttributeDictionary& Attributes() const override
    {
        return *const_cast<TSelf*>(this)->GetCombinedAttributes();
    }

    IAttributeDictionary* MutableAttributes() override
    {
        return GetCombinedAttributes();
    }

    ENodeType GetType() const override
    {
        return ENodeType::Entity;
    }

    std::unique_ptr<ITransactionalNodeFactory> CreateFactory() const override
    {
        THROW_ERROR_EXCEPTION("CreateFactory() method is not supported for access control object principals");
    }

    template <class T>
    TIntrusivePtr<T> GetParent() const
    {
        if (!IsObjectAlive(Owner_)) {
            return nullptr;
        }

        auto* owner = Owner_.Get();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& parentHandler = objectManager->GetHandler(owner);
        auto result = DynamicPointerCast<T>(parentHandler->GetProxy(
            owner,
            /*transaction*/ nullptr));
        YT_VERIFY(result);
        return result;
    }

    ICompositeNodePtr GetParent() const override
    {
        return GetParent<ICompositeNode>();
    }

    void SetParent(const ICompositeNodePtr& /*parent*/) override
    {
        Y_UNREACHABLE();
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(CheckPermission);
        return TBase::DoInvoke(context);
    }

    ISystemAttributeProvider* GetBuiltinAttributeProvider() override
    {
        return this;
    }

    const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys() override
    {
        static const THashSet<TInternedAttributeKey> BuiltinAttributeKeys{
            EInternedAttributeKey::Acl,
            EInternedAttributeKey::EffectiveAcl,
            EInternedAttributeKey::InheritAcl,
            EInternedAttributeKey::Owner
        };
        return BuiltinAttributeKeys;
    }

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        descriptors->push_back(ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Acl)
            .SetWritable(true)
            .SetReplicated(true)
            .SetWritePermission(EPermission::Administer));
        descriptors->push_back(EInternedAttributeKey::EffectiveAcl);
        descriptors->push_back(EInternedAttributeKey::InheritAcl);
        descriptors->push_back(ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Owner)
            .SetWritable(true)
            .SetRemovable(true)
            .SetReplicated(true)
            .SetPresent(IsObjectAlive(Owner_) && Owner_->PrincipalAcd().GetOwner()));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        if (!IsObjectAlive(Owner_)) {
            return false;
        }

        switch (key) {
            case EInternedAttributeKey::Acl:
            case EInternedAttributeKey::EffectiveAcl:
                BuildYsonFluently(consumer)
                    .Value(Owner_->PrincipalAcd().Acl());
                return true;

            case EInternedAttributeKey::InheritAcl:
                BuildYsonFluently(consumer)
                    .Value(false);
                return true;

            case EInternedAttributeKey::Owner: {
                const auto& principalAcd = Owner_->PrincipalAcd();
                if (!principalAcd.GetOwner()) {
                    break;
                }
                BuildYsonFluently(consumer)
                    .Value(principalAcd.GetOwner()->GetName());
                return true;
            }

            default:
                break;
        }

        return false;
    }

    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey /*key*/) override
    {
        return std::nullopt;
    }

    bool SetBuiltinAttribute(TInternedAttributeKey key, const TYsonString& value, bool force) override
    {
        if (!IsObjectAlive(Owner_)) {
            return false;
        }

        auto forwardToParent = [&] (TInternedAttributeKey renamedKey) {
            auto req = TYPathProxy::Set("/@" + renamedKey.Unintern());
            req->set_value(value.ToString());
            req->set_force(force);
            auto context = CreateYPathContext(req->Serialize(), Logger);
            auto typedContext = New<TCtxSet>(context, NRpc::THandlerInvocationOptions());
            YT_VERIFY(typedContext->DeserializeRequest());

            auto parent = GetParent<TAccessControlObjectProxy>();
            parent->SetAttribute(
                renamedKey.Unintern(),
                &typedContext->Request(),
                &typedContext->Response(),
                typedContext);
        };

        switch (key) {
            case EInternedAttributeKey::Acl: {
                forwardToParent(EInternedAttributeKey::PrincipalAcl);
                return true;
            }

            case EInternedAttributeKey::Owner: {
                forwardToParent(EInternedAttributeKey::PrincipalOwner);
                return true;
            }

            default:
                break;
        }

        return false;
    }

    bool RemoveBuiltinAttribute(TInternedAttributeKey key) override
    {
        if (!IsObjectAlive(Owner_)) {
            return false;
        }

        auto forwardToParent = [&] (TInternedAttributeKey renamedKey) {
            auto req = TYPathProxy::Remove("/@" + renamedKey.Unintern());
            auto context = CreateYPathContext(req->Serialize(), Logger);
            auto typedContext = New<TCtxRemove>(context, NRpc::THandlerInvocationOptions());
            YT_VERIFY(typedContext->DeserializeRequest());

            auto parent = GetParent<TAccessControlObjectProxy>();

            parent->RemoveAttribute(
                renamedKey.Unintern(),
                &typedContext->Request(),
                &typedContext->Response(),
                typedContext);
        };

        switch (key) {
            case EInternedAttributeKey::Owner: {
                forwardToParent(EInternedAttributeKey::PrincipalOwner);
                return true;
            }

            default:
                break;
        }

        return false;
    }

private:
    TBootstrap* Bootstrap_;
    TEphemeralObjectPtr<TAccessControlObject> Owner_;
};

DEFINE_YPATH_SERVICE_METHOD(TAccessControlPrincipalProxy, CheckPermission)
{
    TObjectProxyBase::HandleCheckPermissionRequest(
        Bootstrap_,
        context,
        [&] (TUser* user, EPermission permission, TPermissionCheckOptions options) {
            const auto& securityManager = Bootstrap_->GetSecurityManager();
            if (!IsObjectAlive(Owner_)) {
                THROW_ERROR_EXCEPTION("Failed to check permission: access control object no longer exists");
            }
            const auto& acl = Owner_->PrincipalAcd().Acl();
            return securityManager->CheckPermission(
                user,
                permission,
                acl,
                std::move(options));
        });
}

////////////////////////////////////////////////////////////////////////////////

INodePtr TAccessControlObjectProxy::FindChild(const TString& key) const
{
    if (key != ChildKey) {
        return nullptr;
    }

    auto* accessControlObject = const_cast<TAccessControlObject*>(GetThisImpl());

    return New<TAccessControlPrincipalProxy>(
        Bootstrap_,
        TEphemeralObjectPtr<TAccessControlObject>(accessControlObject));
}

////////////////////////////////////////////////////////////////////////////////

class TAccessControlObjectNamespaceProxy
    : public TNonversionedObjectProxyBase<TAccessControlObjectNamespace>
    , public virtual TNodeBase
    , public virtual TMapNodeMixin
    , public THierarchicPermissionValidator<TObject>
{
public:
    YTREE_NODE_TYPE_OVERRIDES(Map)

private:
    using TBase = TNonversionedObjectProxyBase<TAccessControlObjectNamespace>;

public:
    TAccessControlObjectNamespaceProxy(
        TBootstrap* bootstrap,
        TObjectTypeMetadata* metadata,
        TObject* object)
        : TBase(bootstrap, metadata, object)
        , THierarchicPermissionValidator<TObject>(CreatePermissionValidator())
    { }

    TIntrusivePtr<const NYTree::ICompositeNode> AsComposite() const override
    {
        return this;
    }

    TIntrusivePtr<NYTree::ICompositeNode> AsComposite() override
    {
        return this;
    }

    std::unique_ptr<ITransactionalNodeFactory> CreateFactory() const override
    {
        THROW_ERROR_EXCEPTION("CreateFactory() method is not supported for access control object namespaces");
    }

    int GetChildCount() const override
    {
        return ssize(GetThisImpl()->Members());
    }

    void ReplaceChild(const INodePtr& /*oldChild*/, const INodePtr& /*newChild*/) override
    {
        Y_UNREACHABLE();
    }

    void RemoveChild(const INodePtr& /*child*/) override
    {
        THROW_ERROR_EXCEPTION("RemoveChild() method is not supported for access control object namespaces");
    }

    bool RemoveChild(const TString& /*key*/) override
    {
        THROW_ERROR_EXCEPTION("RemoveChild() method is not supported for access control object namespaces");
    }

    std::vector<std::pair<TString, INodePtr>> GetChildren() const override
    {
        const auto* thisImpl = GetThisImpl();
        const auto& children = thisImpl->Members();

        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto& childHandler = objectManager->GetHandler(EObjectType::AccessControlObject);

        std::vector<std::pair<TString, INodePtr>> result;
        result.reserve(ssize(children));
        std::transform(
            children.begin(),
            children.end(),
            std::back_inserter(result),
            [&] (const auto& pair) {
                auto childProxy = childHandler->GetProxy(pair.second, /*transaction*/ nullptr);
                return std::pair(
                    pair.first,
                    DynamicPointerCast<INode>(std::move(childProxy)));
            });
        return result;
    }

    std::vector<TString> GetKeys() const override
    {
        const auto* thisImpl = GetThisImpl();
        const auto& children = thisImpl->Members();

        std::vector<TString> result;
        result.reserve(ssize(children));
        std::transform(
            children.begin(),
            children.end(),
            std::back_inserter(result),
            [&] (const auto& pair) {
                return pair.first;
            });
        return result;
    }

    bool AddChild(const TString& /*key*/, const INodePtr& /*child*/) override
    {
        THROW_ERROR_EXCEPTION("Access control object namespace children cannot be added directly; consider creating an access control object instead");
    }

    std::optional<TString> FindChildKey(const IConstNodePtr& child) override
    {
        auto throwNotChild = [] {
            THROW_ERROR_EXCEPTION("Node is not a child");
        };

        auto accessControlObjectProxy = DynamicPointerCast<const TAccessControlObjectProxy>(child);
        if (!accessControlObjectProxy) {
            throwNotChild();
        }

        auto* accessControlObject = accessControlObjectProxy->GetThisImpl();
        if (const auto* thisImpl = GetThisImpl();
            thisImpl != accessControlObject->Namespace().Get())
        {
            throwNotChild();
        }

        return accessControlObject->GetName();
    }

    INodePtr FindChild(const TString& key) const override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        const auto* thisImpl = GetThisImpl();
        auto* child = thisImpl->FindMember(key);

        if (!child) {
            return nullptr;
        }

        const auto& childHandler = objectManager->GetHandler(child);
        return DynamicPointerCast<INode>(childHandler->GetProxy(child, /*transaction*/ nullptr));
    }

    void Clear() override
    {
        auto* thisImpl = GetThisImpl();
        const auto& objectManager = Bootstrap_->GetObjectManager();

        // NB: zombifying a child will remove it from the member set.
        auto members = thisImpl->Members();
        for (const auto& [name, member] : members) {
            YT_VERIFY(IsObjectAlive(member));
            objectManager->RemoveObject(member);
        }
    }

    ICompositeNodePtr GetParent() const override
    {
        return nullptr;
    }

    void SetParent(const ICompositeNodePtr& /*parent*/) override
    {
        Y_UNREACHABLE();
    }

private:
    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        // Resolve ambiguity between the two base class overrides.
        return TBase::DoInvoke(context);
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        TNodeBase::GetSelf(request, response, context);
    }

    TResolveResult ResolveRecursive(const NYPath::TYPath& path, const IYPathServiceContextPtr& context) override
    {
        return TMapNodeMixin::ResolveRecursive(path, context);
    }

    void RemoveSelf(TReqRemove* request, TRspRemove* response, const TCtxRemovePtr& context) override
    {
        TNodeBase::RemoveSelf(request, response, context);
    }

    void DoRemoveSelf(bool recursive, bool /*force*/) override
    {
        auto* thisImpl = GetThisImpl();

        if (!recursive) {
            YT_LOG_ALERT_IF(
                !thisImpl->Members().empty(),
                "Non-recursive removal of a non-empty access control object namespace detected; skipped "
                "(NamespaceId: %v, Namespace: %v, MemberCount: %v)",
                thisImpl->GetId(),
                thisImpl->GetName(),
                ssize(thisImpl->Members()));
            return;
        }

        Clear();
        YT_VERIFY(thisImpl->Members().empty());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RemoveObject(thisImpl);
    }

    void ListSystemAttributes(std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors) override
    {
        TBase::ListSystemAttributes(descriptors);

        descriptors->push_back(ISystemAttributeProvider::TAttributeDescriptor(EInternedAttributeKey::Name)
            .SetReplicated(true)
            .SetMandatory(true));
    }

    bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override
    {
        const auto* thisImpl = GetThisImpl();

        switch (key) {
            case EInternedAttributeKey::Name:
                BuildYsonFluently(consumer)
                    .Value(thisImpl->GetName());
                return true;

            default:
                break;
        }

        return TBase::GetBuiltinAttribute(key, consumer);
    }

    void ValidatePermission(EPermissionCheckScope scope, EPermission permission, const TString& /*user*/) override
    {
        THierarchicPermissionValidator::ValidatePermission(GetThisImpl(), scope, permission);
    }

    TCompactVector<TObject*, 1> ListDescendantsForPermissionValidation(TObject* object) override
    {
        YT_ASSERT(dynamic_cast<TAccessControlObjectNamespace*>(object));
        auto* thisImpl = static_cast<TAccessControlObjectNamespace*>(object);
        const auto& descendants = thisImpl->Members();

        TCompactVector<TObject*, 1> result;
        result.reserve(ssize(descendants));
        std::transform(
            descendants.begin(),
            descendants.end(),
            std::back_inserter(result),
            [] (const auto& pair) {
                return pair.second;
            });

        return result;
    }

    TObject* GetParentForPermissionValidation(TObject* object) override
    {
        YT_ASSERT(dynamic_cast<TAccessControlObjectNamespace*>(object));
        return nullptr;
    }
};

////////////////////////////////////////////////////////////////////////////////

IObjectProxyPtr CreateAccessControlObjectNamespaceProxy(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TAccessControlObjectNamespace* accessControlObjectNamespace)
{
    return New<TAccessControlObjectNamespaceProxy>(bootstrap, metadata, accessControlObjectNamespace);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
