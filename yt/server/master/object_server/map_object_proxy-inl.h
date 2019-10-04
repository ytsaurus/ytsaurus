#pragma once

#ifndef MAP_OBJECT_PROXY_INL_H_
#error "Direct inclusion of this file is not allowed, include map_object_proxy.h"
// For the sake of sane code completion.
#include "map_object_proxy.h"
#endif

#include "private.h"

#include "map_object_type_handler.h"

#include <yt/client/object_client/helpers.h>
#include <yt/client/object_client/public.h>

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/async_writer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/exception_helpers.h>
#include <yt/core/ytree/faulty_node_factory.h>
#include <yt/core/ytree/helpers.h>

#include <yt/server/lib/misc/interned_attributes.h>

#include <yt/ytlib/cypress_client/cypress_ypath_proxy.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
TNonversionedMapObjectProxyBase<TObject>::TNonversionedMapObjectProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TObject* object)
    : TBase(bootstrap, metadata, object)
{ }

template <class TObject>
TIntrusivePtr<const NYTree::ICompositeNode> TNonversionedMapObjectProxyBase<TObject>::AsComposite() const
{
    return this;
}

template <class TObject>
TIntrusivePtr<NYTree::ICompositeNode> TNonversionedMapObjectProxyBase<TObject>::AsComposite()
{
    return this;
}

template <class TObject>
NYTree::TYPath TNonversionedMapObjectProxyBase<TObject>::GetPath() const
{
    SmallVector<TString, 32> tokens;

    const auto* currentObject = TBase::GetThisImpl();
    for (; currentObject->GetParent(); currentObject = currentObject->GetParent()) {
        tokens.emplace_back(currentObject->GetName());
    }

    auto maybeRootPath = GetTypeHandler()->TryGetRootPath(currentObject);
    auto rootPath = maybeRootPath
        ? *maybeRootPath
        : NObjectClient::FromObjectId(currentObject->GetId());

    TStringBuilder builder;
    builder.AppendString(rootPath);
    for (auto it = tokens.rbegin(); it != tokens.rend(); ++it) {
        builder.AppendChar('/');
        builder.AppendString(*it);
    }

    return builder.Flush();
}

template <class TObject>
NYTree::ICompositeNodePtr TNonversionedMapObjectProxyBase<TObject>::GetParent() const
{
    return DoGetParent();
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::DoGetParent() const
{
    auto* parent = TBase::GetThisImpl()->GetParent();
    return parent ? GetProxy(parent) : nullptr;
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::SetParent(const NYTree::ICompositeNodePtr& parent)
{
    Y_UNREACHABLE();
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::DoInvoke(const NRpc::IServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Create);
    DISPATCH_YPATH_SERVICE_METHOD(Copy);

    if (TNodeBase::DoInvoke(context)) {
        return true;
    }

    if (TBase::DoInvoke(context)) {
        return true;
    }

    return false;
}

template <class TObject>
NYTree::IYPathService::TResolveResult TNonversionedMapObjectProxyBase<TObject>::ResolveRecursive(
    const NYPath::TYPath& path,
    const NRpc::IServiceContextPtr& context)
{
    return NYTree::TMapNodeMixin::ResolveRecursive(path, context);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    NYTree::TNodeBase::GetSelf(request, response, context);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::RemoveSelf(
    TReqRemove* request,
    TRspRemove* response,
    const TCtxRemovePtr& context)
{
    TNodeBase::RemoveSelf(request, response, context);
}

template <class TObject>
int TNonversionedMapObjectProxyBase<TObject>::GetChildCount() const
{
    return TBase::GetThisImpl()->KeyToChild().size();
}

template <class TObject>
std::vector<std::pair<TString, NYTree::INodePtr>> TNonversionedMapObjectProxyBase<TObject>::GetChildren() const
{
    const auto& keyToChild = TBase::GetThisImpl()->KeyToChild();
    std::vector<std::pair<TString, NYTree::INodePtr>> result;
    result.reserve(keyToChild.size());
    for (const auto& [key, child] : keyToChild) {
        result.emplace_back(key, GetProxy(child));
    }

    return result;
}

template <class TObject>
std::vector<TString> TNonversionedMapObjectProxyBase<TObject>::GetKeys() const
{
    const auto& keyToChild = TBase::GetThisImpl()->KeyToChild();
    return NYT::GetKeys(keyToChild);
}

template <class TObject>
NYTree::INodePtr TNonversionedMapObjectProxyBase<TObject>::FindChild(const TString& key) const
{
    auto* child = TBase::GetThisImpl()->FindChild(key);
    return child
        ? GetProxy(child)
        : nullptr;
}

template <class TObject>
std::optional<TString> TNonversionedMapObjectProxyBase<TObject>::FindChildKey(
    const NYTree::IConstNodePtr& child)
{
    auto childProxy = FromNode(child);
    if (!childProxy) {
        return std::nullopt;
    }

    const auto* childImpl = childProxy->GetThisImpl();
    return TBase::GetThisImpl()->GetChildKey(childImpl);
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::AddChild(
    const TString& key,
    const NYTree::INodePtr& child)
{
    THROW_ERROR_EXCEPTION("Use TNonversionedMapObjectFactoryBase::AttachChild() instead");
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateBeforeAttachChild(
    const TString& key,
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& childProxy)
{
    auto* impl = TBase::GetThisImpl();
    if (impl->IsBeingRemoved()) {
        THROW_ERROR_EXCEPTION("Cannot attach new children to an object being removed (LifeStage: %Qlv, Id: %v)",
            impl->GetLifeStage(),
            impl->GetId());
    }

    ValidateChildName(key);
    ValidateAttachChildDepth(childProxy);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateAfterAttachChild(
    const TString& key,
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& childProxy)
{ }

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ReplaceChild(
    const NYTree::INodePtr& oldChild,
    const NYTree::INodePtr& newChild)
{
    Y_UNREACHABLE();
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::RemoveChild(const NYTree::INodePtr& child)
{
    YT_VERIFY(child);
    auto childProxy = FromNode(child);
    childProxy->ValidateRemoval();
    DoRemoveChild(childProxy);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::DoRemoveChild(
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& childProxy)
{
    YT_VERIFY(childProxy);
    auto* childImpl = childProxy->GetThisImpl();
    YT_VERIFY(!childImpl->IsBeingCreated());
    if (childImpl->IsBeingRemoved()) {
        return;
    }

    childProxy->RemoveChildren();
    TBase::Bootstrap_->GetObjectManager()->RemoveObject(childImpl);
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::RemoveChild(const TString& key)
{
    auto child = FindChild(key);
    if (!child) {
        return false;
    }

    RemoveChild(child);
    return true;
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::RemoveChildren()
{
    auto children = GetChildren();
    std::sort(children.begin(), children.end(), [] (const auto& lhs, const auto& rhs) {
        return lhs.first < rhs.first;
    });

    for (const auto& [_, child] : children) {
        DoRemoveChild(FromNode(child));
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::AttachChild(
    const TString& key,
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& childProxy) noexcept
{
    YT_VERIFY(childProxy);
    auto* impl = TBase::GetThisImpl();
    auto* childImpl = childProxy->GetThisImpl();

    impl->AttachChild(key, childImpl);
    TBase::Bootstrap_->GetObjectManager()->RefObject(impl);
    GetTypeHandler()->RegisterName(key, childImpl);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::DetachChild(
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& childProxy) noexcept
{
    YT_VERIFY(childProxy);
    auto* impl = TBase::GetThisImpl();
    auto* childImpl = childProxy->GetThisImpl();
    YT_VERIFY(childImpl->GetParent() == impl);
    auto key = impl->GetChildKey(childImpl);
    GetTypeHandler()->UnregisterName(key, childImpl);
    impl->DetachChild(childImpl);
    TBase::Bootstrap_->GetObjectManager()->UnrefObject(impl);
}

template <class TObject>
std::unique_ptr<NYTree::ITransactionalNodeFactory>
TNonversionedMapObjectProxyBase<TObject>::CreateFactory() const
{
    THROW_ERROR_EXCEPTION("CreateFactory() method is not supported for nonversioned map objects");
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::Clear()
{
    auto keys = GetKeys();
    std::sort(keys.begin(), keys.end());

    std::vector<TIntrusivePtr<TNonversionedMapObjectProxyBase>> children;
    children.reserve(keys.size());
    for (const auto& key : keys) {
        children.emplace_back(FromNodeOrThrow(GetChild(key)));
    }

    for (auto& child : children) {
        child->ValidateRemoval();
    }
    for (auto& child : children) {
        DoRemoveChild(child);
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::SetSelf(
    TReqSet* request,
    TRspSet* response,
    const TCtxSetPtr& context)
{
    TSupportsSet::SetSelf(request, response, context);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::SetRecursive(
    const NYPath::TYPath& path,
    TReqSet* request,
    TRspSet* response,
    const TCtxSetPtr& context)
{
    TSupportsSet::SetRecursive(path, request, response, context);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidatePermission(
    NYTree::EPermissionCheckScope scope,
    NYTree::EPermission permission,
    const TString& /* user */)
{
    ValidatePermission(TBase::GetThisImpl(), scope, permission);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidatePermission(
    TObject* object,
    NYTree::EPermissionCheckScope scope,
    NYTree::EPermission permission)
{
    if (Any(scope & NYTree::EPermissionCheckScope::This)) {
        TBase::ValidatePermission(object, permission);
    }

    if (Any(scope & NYTree::EPermissionCheckScope::Parent) && TBase::GetThisImpl()->GetParent()) {
        TBase::ValidatePermission(object->GetParent(), permission);
    }

    if (Any(scope & NYTree::EPermissionCheckScope::Descendants)) {
        for (const auto& [_, child] : object->KeyToChild()) {
            ValidatePermission(
                child,
                NYTree::EPermissionCheckScope::This | NYTree::EPermissionCheckScope::Descendants,
                permission);
        }
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ListSystemAttributes(
    std::vector<NYTree::ISystemAttributeProvider::TAttributeDescriptor> *descriptors)
{
    using TAttributeDescriptor = NYTree::ISystemAttributeProvider::TAttributeDescriptor;

    TObjectProxyBase::ListSystemAttributes(descriptors);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
        .SetWritable(true)
        .SetPresent(TBase::GetThisImpl()->GetParent()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ParentName)
        .SetWritable(true)
        .SetPresent(TBase::GetThisImpl()->GetParent()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Path));
    // YYY(kiselyovp)++ hm, how do i replicate the tree structure >_<
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::GetBuiltinAttribute(
    NYTree::TInternedAttributeKey key,
    NYson::IYsonConsumer* consumer)
{
    const auto* impl = TBase::GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::Name: {
            auto name = impl->GetName();
            NYTree::BuildYsonFluently(consumer)
                .Value(name);
            return true;
        }

        case EInternedAttributeKey::ParentName: {
            auto* parent = impl->GetParent();
            if (!parent) {
                return false;
            }
            auto parentName = parent->GetName();
            NYTree::BuildYsonFluently(consumer)
                .Value(parentName);
            return true;
        }

        case EInternedAttributeKey::Path:
            NYTree::BuildYsonFluently(consumer)
                .Value(GetPath());
            return true;

        default:
            break;
    }

    return TBase::GetBuiltinAttribute(key, consumer);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::RenameSelf(const TString& newName)
{
    auto* impl = TBase::GetThisImpl();
    auto* parent = impl->GetParent();
    if (!parent) {
        THROW_ERROR_EXCEPTION("Cannot rename root %v", impl->GetType());
    }
    auto oldName = impl->GetName();
    if (oldName == newName) {
        return;
    }

    GetProxy(parent)->ValidateChildName(newName);
    DoRenameSelf(newName);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::DoRenameSelf(const TString& newName)
{
    auto* impl = TBase::GetThisImpl();
    auto oldName = impl->GetName();
    auto handler = GetTypeHandler();
    auto* parent = impl->GetParent();

    handler->UnregisterName(oldName, impl);
    parent->RenameChild(impl, newName);
    handler->RegisterName(newName, impl);
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::SetBuiltinAttribute(
    NYTree::TInternedAttributeKey key,
    const NYson::TYsonString& value)
{
    switch (key) {
        case EInternedAttributeKey::Name: {
            auto newName = NYTree::ConvertTo<TString>(value);
            RenameSelf(newName);
            return true;
        }

        case EInternedAttributeKey::ParentName: {
            auto* impl = TBase::GetThisImpl();
            if (!impl->GetParent()) {
                THROW_ERROR_EXCEPTION("Cannot change parent for a nameless %v", impl->GetType())
                    << TErrorAttribute("id", impl->GetId());
            }

            auto newParentName = NYTree::ConvertTo<TString>(value);
            auto newParent = ResolveNameOrThrow(newParentName);
            auto name = impl->GetName();

            auto req = NCypressClient::TCypressYPathProxy::Copy("/" + name);
            req->set_source_path(GetShortPath());
            req->set_mode(static_cast<int>(NCypressClient::ENodeCloneMode::Move));
            NYTree::SyncExecuteVerb(newParent, req);
            return true;
        }

        default:
            break;
    }

    return TObjectProxyBase::SetBuiltinAttribute(key, value);
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>
TNonversionedMapObjectProxyBase<TObject>::GetProxy(NCellMaster::TBootstrap* bootstrap, TObject* object)
{
    const auto& objectManager = bootstrap->GetObjectManager();
    auto proxy = objectManager->GetProxy(object, nullptr);
    // XXX(kiselyovp) ugly as heck
    auto* result = dynamic_cast<TSelf*>(proxy.Get());
    YT_VERIFY(result);
    return result;
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>
TNonversionedMapObjectProxyBase<TObject>::GetProxy(TObject* object) const
{
    return GetProxy(TBase::Bootstrap_, object);
}

template <class TObject>
TString TNonversionedMapObjectProxyBase<TObject>::GetShortPath() const
{
    return NObjectClient::FromObjectId(TObjectProxyBase::GetId());
}

template <class TObject>
NObjectServer::TObject* TNonversionedMapObjectProxyBase<TObject>::ResolvePathToNonversionedObject(
    const NYPath::TYPath& path) const
{
    // XXX(kiselyovp) Using this crutch because it's easier than fixing
    // TObjectManager::ResolvePathToObject() for map objects.
    const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
    auto rootService = objectManager->GetRootService();
    auto req = NObjectClient::TObjectYPathProxy::GetBasicAttributes(path);
    auto rsp = SyncExecuteVerb(rootService, req);
    auto objectId = FromProto<TObjectId>(rsp->object_id());
    return objectManager->GetObjectOrThrow(objectId);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateAttachChildDepth(
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& child)
{
    YT_VERIFY(child);
    GetTypeHandler()->ValidateAttachChildDepth(TBase::GetThisImpl(), child->GetThisImpl());
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateRemoval()
{
    auto* impl = TBase::GetObject();
    if (impl->IsBeingRemoved()) {
        return;
    }
    if (impl->IsBeingCreated()) {
        THROW_ERROR_EXCEPTION("Invalid life stage during object removal (LifeStage: %Qlv, Id: %v)",
            impl->GetLifeStage(),
            impl->GetId());
    }

    for (auto [_, child] : GetChildren()) {
        FromNodeOrThrow(child)->ValidateRemoval();
    }

    auto handler = GetTypeHandler();
    auto flags = handler->GetFlags();
    if (None(flags & ETypeFlags::TwoPhaseRemoval)) {
        const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
        auto refCount = objectManager->GetObjectRefCounter(TBase::GetObject());
        auto expectedRefCount = GetChildCount() + 1;
        if (refCount != expectedRefCount) {
            THROW_ERROR_EXCEPTION("%v is in use", TBase::GetThisImpl()->GetObjectName());
        }
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateChildName(const TString& newChildName)
{
    if (newChildName.empty()) {
        THROW_ERROR_EXCEPTION("Name cannot be empty");
    }
    if (newChildName.find("/") != TString::npos) {
        THROW_ERROR_EXCEPTION("Name cannot contain slashes");
    }
    if (newChildName.StartsWith(NObjectClient::ObjectIdPathPrefix)) {
        THROW_ERROR_EXCEPTION("Name cannot start with %Qv",
            NObjectClient::ObjectIdPathPrefix);
    }

    auto* impl = TBase::GetThisImpl();
    if (impl->KeyToChild().count(newChildName) != 0) {
        THROW_ERROR_EXCEPTION(
            "Object %Qv already has a child %Qv",
            impl->GetName(),
            newChildName);
    }
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::FromNode(
    const NYTree::INodePtr& node)
{
    return dynamic_cast<TSelf*>(node.Get());
}

template <class TObject>
TIntrusivePtr<const TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::FromNode(
    const NYTree::IConstNodePtr& node)
{
    return dynamic_cast<const TSelf*>(node.Get());
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::FromNodeOrThrow(
    const NYTree::INodePtr& node)
{
    auto result = FromNode(node);
    if (node && !result) {
        THROW_ERROR_EXCEPTION("Failed to convert node to map object proxy");
    }
    return result;
}

template <class TObject>
TIntrusivePtr<const TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::FromNodeOrThrow(
    const NYTree::IConstNodePtr& node)
{
    auto result = FromNode(node);
    if (node && !result) {
        THROW_ERROR_EXCEPTION("Failed to convert node to map object proxy");
    }
    return result;
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectTypeHandlerBase<TObject>>
    TNonversionedMapObjectProxyBase<TObject>::GetTypeHandler() const
{
    auto& objectManager = TBase::Bootstrap_->GetObjectManager();
    auto handler = objectManager->GetHandler(TBase::GetThisImpl());

    auto* mapObjectHandler = dynamic_cast<TTypeHandler*>(handler.Get());
    YT_VERIFY(mapObjectHandler);
    return mapObjectHandler;
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::SetImmediateChild(
    TNonversionedMapObjectFactoryBase<TObject>* factory,
    const NYPath::TYPath& path,
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& child)
{
    const auto& [key, _] = PrepareSetChild(
        NYTree::GetFaultyNodeFactory(),
        path,
        child,
        false /* recursive */);

    factory->AttachChild(this, key, child);
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::Create(
    EObjectType type,
    const TString& path,
    NYTree::IAttributeDictionary* attributes)
{
    TObjectProxyBase::DeclareMutating();

    if (type != TBase::GetThisImpl()->GetType()) {
        THROW_ERROR_EXCEPTION("Cannot create an object of the type %Qv, expected type %Qv",
            type,
            TBase::GetThisImpl()->GetType());
    }
    if (path.empty()) {
        ThrowAlreadyExists(this);
    }

    ValidatePermission(
        NYTree::EPermissionCheckScope::This,
        NSecurityServer::EPermission::Write | NSecurityServer::EPermission::ModifyChildren);

    if (attributes && (attributes->Contains("acl") || attributes->Contains("inherit_acl"))) {
        ValidatePermission(
            TBase::GetThisImpl(),
            NYTree::EPermissionCheckScope::This,
            NSecurityServer::EPermission::Administer);
    }

    auto factory = CreateObjectFactory();
    auto* object = factory->CreateObject(attributes);
    SetImmediateChild(factory.get(), path, GetProxy(object));
    factory->Commit();

    return GetProxy(object);
}

template <class TObject>
DEFINE_YPATH_SERVICE_METHOD(TNonversionedMapObjectProxyBase<TObject>, Create)
{
    auto type = EObjectType(request->type());
    auto ignoreExisting = request->ignore_existing();
    auto recursive = request->recursive();
    auto force = request->force();
    const auto& path = NYTree::GetRequestTargetYPath(context->RequestHeader());

    std::unique_ptr<NYTree::IAttributeDictionary> explicitAttributes;
    if (request->has_node_attributes()) {
        explicitAttributes = NYTree::FromProto(request->node_attributes());
    }

    if (recursive) {
        THROW_ERROR_EXCEPTION("\"recursive\" option is not supported for nonversioned map objects");
    }
    if (force) {
        THROW_ERROR_EXCEPTION("\"force\" option is not supported for nonversioned map objects");
    }
    if (ignoreExisting) {
        THROW_ERROR_EXCEPTION("\"ignore_existing\" option is not supported for nonversioned map objects");
    }

    context->SetRequestInfo("Type: %v, IgnoreExisting: %v, Recursive: %v, Force: %v",
        type,
        ignoreExisting,
        recursive,
        force);

    auto proxy = Create(type, path, explicitAttributes.get());
    const auto& objectId = proxy->GetId();

    response->set_cell_tag(TBase::Bootstrap_->GetCellTag());
    ToProto(response->mutable_node_id(), objectId);
    context->SetResponseInfo("ObjectId: %v",
        objectId);

    context->Reply();
}

template <class TObject>
DEFINE_YPATH_SERVICE_METHOD(TNonversionedMapObjectProxyBase<TObject>, Copy)
{
    TObjectProxyBase::DeclareMutating();

    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    // COMPAT(babenko)
    const auto& sourcePath = ypathExt.additional_paths_size() == 1
        ? ypathExt.additional_paths(0)
        : request->source_path();

    auto mode = CheckedEnumCast<NCypressClient::ENodeCloneMode>(request->mode());
    auto recursive = request->recursive();
    auto ignoreExisting = request->ignore_existing();
    auto force = request->force();
    auto targetPath = NYTree::GetRequestTargetYPath(context->RequestHeader());
    auto* impl = TBase::GetThisImpl();

    auto* sourceObject = ResolvePathToNonversionedObject(sourcePath);
    if (sourceObject->GetType() != TBase::GetObject()->GetType()) {
        THROW_ERROR_EXCEPTION("Cannot copy or move an object of the type %Qv, expected type %Qv",
            sourceObject->GetType(),
            impl->GetType());
    }

    if (recursive) {
        THROW_ERROR_EXCEPTION("\"recursive\" option is not supported for nonversioned map objects");
    }
    if (force) {
        THROW_ERROR_EXCEPTION("\"force\" option is not supported for nonversioned map objects");
    }
    if (ignoreExisting && mode == NCypressClient::ENodeCloneMode::Move) {
        THROW_ERROR_EXCEPTION("Cannot specify \"ignore_existing\" for move operation");
    }

    context->SetRequestInfo("SourcePath: %v, Mode: %v, Recursive: %v, IgnoreExisting: %v, Force: %v",
        sourcePath,
        mode,
        recursive,
        ignoreExisting,
        force);

    bool replace = targetPath.empty();
    if (replace) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(this);
        }
        ToProto(response->mutable_node_id(), impl->GetId());
        context->SetResponseInfo("ExistingObjectId: %v",
            impl->GetId());
        context->Reply();
        return;
    }

    auto* sourceImpl = sourceObject->template As<TObject>();
    auto sourceProxy = GetProxy(sourceImpl);

    for (auto* current = impl; current; current = current->GetParent()) {
        if (current == sourceImpl) {
            THROW_ERROR_EXCEPTION("Cannot copy or move an object to its descendant");
        }
    }

    ValidatePermission(
        NYTree::EPermissionCheckScope::This,
        NSecurityServer::EPermission::Write | NSecurityServer::EPermission::ModifyChildren);

    ValidatePermission(
        sourceImpl,
        NYTree::EPermissionCheckScope::This | NYTree::EPermissionCheckScope::Descendants,
        NSecurityServer::EPermission::Read);

    auto sourceParent = sourceProxy->DoGetParent();
    if (mode == NCypressClient::ENodeCloneMode::Move) {
        if (!sourceParent) {
            NYTree::ThrowCannotRemoveNode(sourceProxy);
        }
        ValidatePermission(
            sourceImpl,
            NYTree::EPermissionCheckScope::This | NYTree::EPermissionCheckScope::Descendants,
            NSecurityServer::EPermission::Remove);
        ValidatePermission(
            sourceImpl,
            NYTree::EPermissionCheckScope::Parent,
            NSecurityServer::EPermission::Write | NSecurityServer::EPermission::ModifyChildren);
    }

    auto factory = CreateObjectFactory();

    TSelfPtr clonedProxy;
    if (mode == NCypressClient::ENodeCloneMode::Move) {
        factory->DetachChild(sourceParent, sourceProxy);
        clonedProxy = sourceProxy;
    } else {
        auto* clonedObject = factory->CloneObject(sourceImpl, NCypressServer::ENodeCloneMode::Copy);
        clonedProxy = GetProxy(clonedObject);
    }

    SetImmediateChild(factory.get(), targetPath, clonedProxy);

    factory->Commit();

    ToProto(response->mutable_node_id(), clonedProxy->GetId());

    context->SetResponseInfo("NodeId: %v", clonedProxy->GetId());

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
TObject* TNonversionedMapObjectFactoryBase<TObject>::CreateObject(NYTree::IAttributeDictionary* attributes)
{
    auto* object = DoCreateObject(attributes);
    CreatedObjects_.push_back(object);
    return object;
}

template <class TObject>
TObject* TNonversionedMapObjectFactoryBase<TObject>::CloneObject(
    TObject* /* object */,
    NCypressServer::ENodeCloneMode /* mode */)
{
    THROW_ERROR_EXCEPTION("Nonversioned map objects don't support cloning");
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::Commit()
{
    for (const auto& event : EventLog_) {
        CommitEvent(event);
    }
    EventLog_.clear();
    CleanupCreatedObjects(false);
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::Rollback()
{
    for (auto it = EventLog_.rbegin(); it != EventLog_.rend(); ++it) {
        RollbackEvent(*it);
    }
    EventLog_.clear();
    CleanupCreatedObjects(true);
}

template <class TObject>
TNonversionedMapObjectFactoryBase<TObject>::TNonversionedMapObjectFactoryBase(
    NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

template <class TObject>
TNonversionedMapObjectFactoryBase<TObject>::~TNonversionedMapObjectFactoryBase()
{
    Rollback();
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::LogEvent(
    const TFactoryEvent& event)
{
    EventLog_.push_back(event);
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::AttachChild(
    const TProxyPtr& parent,
    const TString& key,
    const TProxyPtr& child)
{
    parent->ValidateBeforeAttachChild(key, child);

    parent->AttachChild(key, child);
    LogEvent({
        EFactoryEventType::AttachChild,
        parent,
        key,
        child
    });

    parent->ValidateAfterAttachChild(key, child);
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::DetachChild(const TProxyPtr& parent, const TProxyPtr& child)
{
    Bootstrap_->GetObjectManager()->RefObject(parent->GetObject());
    LogEvent({
        EFactoryEventType::RefObject,
        parent,
        TString(), /* key */
        child /* child */
    });

    auto key = parent->GetChildKeyOrThrow(child);
    parent->DetachChild(child);
    LogEvent({
        EFactoryEventType::DetachChild,
        parent,
        key,
        child
    });
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::CleanupCreatedObjects(bool removeObjects)
{
    if (removeObjects) {
        for (auto* object: CreatedObjects_) {
            YT_VERIFY(object->GetObjectRefCounter() == 1);
            Bootstrap_->GetObjectManager()->UnrefObject(object);
        }
    }

    CreatedObjects_.clear();
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::CommitEvent(const TFactoryEvent& event)
{
    if (event.Type == EFactoryEventType::RefObject) {
        Bootstrap_->GetObjectManager()->UnrefObject(event.Parent->GetObject());
    }
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::RollbackEvent(const TFactoryEvent& event)
{
    try {
        switch (event.Type) {
            case EFactoryEventType::RefObject:
                Bootstrap_->GetObjectManager()->UnrefObject(event.Parent->GetObject());
                break;
            case EFactoryEventType::AttachChild:
                event.Parent->DetachChild(event.Child);
                break;
            case EFactoryEventType::DetachChild:
                event.Parent->AttachChild(event.Key, event.Child);
                break;
            default:
                Y_UNREACHABLE();
        }
    } catch (const std::exception& ex) {
        auto& Logger = NObjectServer::ObjectServerLogger;
        YT_LOG_FATAL(ex, "Unhandled exception during rollback of factory event of the type %Qv", event.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
