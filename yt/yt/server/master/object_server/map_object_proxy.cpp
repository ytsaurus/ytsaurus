#include "private.h"

#include "map_object_proxy.h"
#include "map_object_type_handler.h"

#include <yt/yt/client/object_client/helpers.h>
#include <yt/yt/client/object_client/public.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/yson/async_writer.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/exception_helpers.h>
#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/scheduler_pool_server/scheduler_pool.h>

#include <yt/yt/ytlib/cypress_client/cypress_ypath_proxy.h>

namespace NYT::NObjectServer {

using namespace NCypressClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
TNonversionedMapObjectProxyBase<TObject>::TNonversionedMapObjectProxyBase(
    NCellMaster::TBootstrap* bootstrap,
    TObjectTypeMetadata* metadata,
    TObject* object)
    : TBase(bootstrap, metadata, object)
    , THierarchicPermissionValidator<TObject>(TBase::CreatePermissionValidator())
{ }

template <class TObject>
TIntrusivePtr<const ICompositeNode> TNonversionedMapObjectProxyBase<TObject>::AsComposite() const
{
    return this;
}

template <class TObject>
TIntrusivePtr<ICompositeNode> TNonversionedMapObjectProxyBase<TObject>::AsComposite()
{
    return this;
}

template <class TObject>
TYPath TNonversionedMapObjectProxyBase<TObject>::GetPath() const
{
    TCompactVector<TString, 32> tokens;

    const auto* currentObject = TBase::GetThisImpl();
    for (; currentObject->GetParent(); currentObject = currentObject->GetParent()) {
        tokens.emplace_back(currentObject->GetName());
    }

    auto rootPath = currentObject->IsRoot()
        ? GetTypeHandler()->GetRootPath(currentObject)
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
ICompositeNodePtr TNonversionedMapObjectProxyBase<TObject>::GetParent() const
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
void TNonversionedMapObjectProxyBase<TObject>::SetParent(const ICompositeNodePtr& /*parent*/)
{
    Y_UNREACHABLE();
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::DoInvoke(const IYPathServiceContextPtr& context)
{
    DISPATCH_YPATH_SERVICE_METHOD(Create);
    DISPATCH_YPATH_SERVICE_METHOD(Copy);

    if (TBase::DoInvoke(context)) {
        return true;
    }

    return false;
}

template <class TObject>
IYPathService::TResolveResult TNonversionedMapObjectProxyBase<TObject>::ResolveRecursive(
    const TYPath& path,
    const IYPathServiceContextPtr& context)
{
    return TMapNodeMixin::ResolveRecursive(path, context);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::GetSelf(
    TReqGet* request,
    TRspGet* response,
    const TCtxGetPtr& context)
{
    TNodeBase::GetSelf(request, response, context);
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
void TNonversionedMapObjectProxyBase<TObject>::SetSelf(
    TReqSet* request,
    TRspSet* response,
    const TCtxSetPtr& context)
{
    TSupportsSet::SetSelf(request, response, context);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::SetRecursive(
    const TYPath& path,
    TReqSet* /*request*/,
    TRspSet* /*response*/,
    const TCtxSetPtr& /*context*/)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Literal);

    ThrowNoSuchChildKey(this, tokenizer.GetLiteralValue());
}

template <class TObject>
int TNonversionedMapObjectProxyBase<TObject>::GetChildCount() const
{
    return TBase::GetThisImpl()->KeyToChild().size();
}

template <class TObject>
std::vector<std::pair<TString, INodePtr>> TNonversionedMapObjectProxyBase<TObject>::GetChildren() const
{
    const auto& keyToChild = TBase::GetThisImpl()->KeyToChild();
    std::vector<std::pair<TString, INodePtr>> result;
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
INodePtr TNonversionedMapObjectProxyBase<TObject>::FindChild(const TString& key) const
{
    auto* child = TBase::GetThisImpl()->FindChild(key);
    return child
        ? GetProxy(child)
        : nullptr;
}

template <class TObject>
std::optional<TString> TNonversionedMapObjectProxyBase<TObject>::FindChildKey(
    const IConstNodePtr& child)
{
    auto childProxy = FromNode(child);
    const auto* childImpl = childProxy->GetThisImpl();
    return TBase::GetThisImpl()->GetChildKey(childImpl);
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::AddChild(
    const TString& /*key*/,
    const INodePtr& /*child*/)
{
    THROW_ERROR_EXCEPTION("Use TNonversionedMapObjectFactoryBase::AttachChild() instead");
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateBeforeAttachChild(
    const TString& key,
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& childProxy)
{
    auto* impl = TBase::GetThisImpl();

    const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
    objectManager->ValidateObjectLifeStage(impl);

    auto* childImpl = childProxy->GetThisImpl();
    if (childImpl->IsRoot()) {
        THROW_ERROR_EXCEPTION("Root object cannot have a parent")
            << TErrorAttribute("id", childImpl->GetId());
    }

    ValidateChildName(key);
    ValidateAttachChildDepth(childProxy);
    ValidateAttachChildSubtreeSize(childProxy);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateAfterAttachChild(
    const TString& /*key*/,
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& /*childProxy*/)
{ }

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ReplaceChild(
    const INodePtr& /*oldChild*/,
    const INodePtr& /*newChild*/)
{
    Y_UNREACHABLE();
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::RemoveChild(const INodePtr& child)
{
    YT_VERIFY(child);
    auto childProxy = FromNode(child);
    if (childProxy->GetThisImpl()->GetParent() != TBase::GetThisImpl()) {
        THROW_ERROR_EXCEPTION("Node is not a child");
    }
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
    const auto& keyToChild = TBase::GetThisImpl()->KeyToChild();
    for (const auto& [_, childImpl] : SortHashMapByKeys(keyToChild)) {
        DoRemoveChild(GetProxy(childImpl));
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
std::unique_ptr<ITransactionalNodeFactory>
TNonversionedMapObjectProxyBase<TObject>::CreateFactory() const
{
    THROW_ERROR_EXCEPTION("CreateFactory() method is not supported for nonversioned map objects");
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::Clear()
{
    const auto& sortedKeyToChild = SortHashMapByKeys(TBase::GetThisImpl()->KeyToChild());

    std::vector<TIntrusivePtr<TNonversionedMapObjectProxyBase>> children;
    children.reserve(sortedKeyToChild.size());
    for (const auto& [_, childImpl] : sortedKeyToChild) {
        children.emplace_back(GetProxy(childImpl));
    }

    for (const auto& child : children) {
        child->ValidateRemoval();
    }
    for (const auto& child : children) {
        DoRemoveChild(child);
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidatePermission(
    EPermissionCheckScope scope,
    EPermission permission,
    const TString& /*user*/)
{
    ValidatePermission(TBase::GetThisImpl(), scope, permission);
}

template <class TObject>
TCompactVector<TObject*, 1> TNonversionedMapObjectProxyBase<TObject>::ListDescendantsForPermissionValidation(
    TObject* object)
{
    return AccumulateOverMapObjectSubtree(
        object,
        TCompactVector<TObject*, 1>(),
        [root = object] (auto* currentObject, auto* descendants) {
            if (currentObject != root) {
                descendants->push_back(currentObject);
            }
        });
}

template <class TObject>
TObject* TNonversionedMapObjectProxyBase<TObject>::GetParentForPermissionValidation(TObject* object)
{
    return object->GetParent();
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ListSystemAttributes(
    std::vector<ISystemAttributeProvider::TAttributeDescriptor>* descriptors)
{
    using TAttributeDescriptor = ISystemAttributeProvider::TAttributeDescriptor;

    TObjectProxyBase::ListSystemAttributes(descriptors);
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Name)
        .SetWritable(true)
        .SetReplicated(true)
        .SetMandatory(true));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::ParentName)
        .SetWritable(true)
        .SetReplicated(true)
        .SetPresent(TBase::GetThisImpl()->GetParent()));
    descriptors->push_back(TAttributeDescriptor(EInternedAttributeKey::Path));
}

template <class TObject>
bool TNonversionedMapObjectProxyBase<TObject>::GetBuiltinAttribute(
    TInternedAttributeKey key,
    NYson::IYsonConsumer* consumer)
{
    const auto* impl = TBase::GetThisImpl();

    switch (key) {
        case EInternedAttributeKey::Name: {
            auto name = impl->GetName();
            BuildYsonFluently(consumer)
                .Value(name);
            return true;
        }

        case EInternedAttributeKey::ParentName: {
            auto* parent = impl->GetParent();
            if (!parent) {
                return false;
            }
            auto parentName = parent->GetName();
            BuildYsonFluently(consumer)
                .Value(parentName);
            return true;
        }

        case EInternedAttributeKey::Path:
            BuildYsonFluently(consumer)
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
        THROW_ERROR_EXCEPTION("Cannot rename %v as it has no parent", impl->GetLowercaseObjectName());
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
    TInternedAttributeKey key,
    const NYson::TYsonString& value,
    bool force)
{
    switch (key) {
        case EInternedAttributeKey::Name: {
            auto newName = ConvertTo<TString>(value);
            RenameSelf(newName);
            return true;
        }

        case EInternedAttributeKey::ParentName: {
            auto* impl = TBase::GetThisImpl();
            if (!impl->GetParent()) {
                THROW_ERROR_EXCEPTION("Cannot change parent for a nameless %Qlv", impl->GetType())
                    << TErrorAttribute("id", impl->GetId());
            }

            auto newParentName = ConvertTo<TString>(value);
            auto newParent = ResolveNameOrThrow(newParentName);
            auto name = impl->GetName();

            newParent->Copy(
                GetShortPath(),
                "/" + name,
                ENodeCloneMode::Move,
                false /*ignoreExisting*/);
            return true;
        }

        default:
            break;
    }

    return TObjectProxyBase::SetBuiltinAttribute(key, value, force);
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>
TNonversionedMapObjectProxyBase<TObject>::GetProxy(TObject* object) const
{
    return GetTypeHandler()->GetMapObjectProxy(object);
}

template <class TObject>
TString TNonversionedMapObjectProxyBase<TObject>::GetShortPath() const
{
    return NObjectClient::FromObjectId(TObjectProxyBase::GetId());
}

template <class TObject>
NObjectServer::TObject* TNonversionedMapObjectProxyBase<TObject>::ResolvePathToNonversionedObject(
    const TYPath& path) const
{
    // XXX(kiselyovp) Using this crutch because it's easier than fixing
    // IObjectManager::ResolvePathToObject() for map objects.
    const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
    auto rootService = objectManager->GetRootService();
    auto req = NObjectClient::TObjectYPathProxy::GetBasicAttributes(path);
    auto rsp = SyncExecuteVerb(rootService, req);
    auto objectId = FromProto<TObjectId>(rsp->object_id());
    return objectManager->GetObjectOrThrow(objectId);
}

template <class TObject>
int TNonversionedMapObjectProxyBase<TObject>::GetTopmostAncestorSubtreeSize(const TObject* object) const
{
    YT_VERIFY(object);
    YT_VERIFY(!object->IsRoot());

    while (!object->GetParent()->IsRoot()) {
        object = object->GetParent();
    }

    return object->GetSubtreeSize();
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateAttachChildSubtreeSize(
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& child)
{
    YT_VERIFY(child);

    auto* impl = TBase::GetThisImpl();
    if (impl->IsRoot()) {
        return;
    }
    auto subtreeSizeLimit = GetTypeHandler()->GetSubtreeSizeLimit();
    if (!subtreeSizeLimit) {
        return;
    }

    YT_ASSERT(!child->GetParent());
    auto newSubtreeSize = child->GetThisImpl()->GetSubtreeSize() + GetTopmostAncestorSubtreeSize(impl);
    if (newSubtreeSize > subtreeSizeLimit) {
        THROW_ERROR_EXCEPTION("Subtree size limit exceeded for %v", impl->GetLowercaseObjectName())
            << TErrorAttribute("new_subtree_size", newSubtreeSize)
            << TErrorAttribute("subtree_size_limit", subtreeSizeLimit);
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateAttachChildDepth(
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& child)
{
    YT_VERIFY(child);
    auto* impl = TBase::GetThisImpl();
    auto depthLimit = GetTypeHandler()->GetDepthLimit();
    if (!depthLimit) {
        return;
    }

    auto heightLimit = *depthLimit - GetDepth(impl) - 1;
    ValidateHeightLimit(child->GetThisImpl(), heightLimit);
}

template <class TObject>
int TNonversionedMapObjectProxyBase<TObject>::GetDepth(const TObject* object) const
{
    YT_VERIFY(object);
    auto depth = 0;
    for (auto* current = object->GetParent(); current; current = current->GetParent()) {
        ++depth;
    }

    return depth;
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateHeightLimit(
    const TObject* root,
    int heightLimit) const
{
    YT_VERIFY(root);
    if (heightLimit < 0) {
        THROW_ERROR_EXCEPTION("%v tree height limit exceeded", root->GetType());
    }
    for (const auto& [child, _] : root->ChildToKey()) {
        ValidateHeightLimit(child, heightLimit - 1);
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateRemoval()
{
    auto* impl = TBase::GetThisImpl();
    if (impl->IsBeingRemoved()) {
        return;
    }
    if (impl->IsBeingCreated()) {
        THROW_ERROR_EXCEPTION(
            NObjectClient::EErrorCode::InvalidObjectLifeStage,
            "Invalid life stage during object removal")
            << TErrorAttribute("life_stage", impl->GetLifeStage())
            << TErrorAttribute("id", impl->GetId());
    }

    for (const auto& [_, childImpl] : impl->KeyToChild()) {
        GetProxy(childImpl)->ValidateRemoval();
    }

    auto handler = GetTypeHandler();
    auto flags = handler->GetFlags();
    if (None(flags & ETypeFlags::TwoPhaseRemoval)) {
        auto refCount = TBase::GetObject()->GetObjectRefCounter(/*flushUnrefs*/ true);
        auto expectedRefCount = GetChildCount() + 1;
        if (refCount != expectedRefCount) {
            THROW_ERROR_EXCEPTION("%v is in use", TBase::GetThisImpl()->GetCapitalizedObjectName());
        }
    }
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateChildName(const TString& childName)
{
    GetTypeHandler()->ValidateObjectName(childName);
    ValidateChildNameAvailability(childName);
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::ValidateChildNameAvailability(const TString& childName)
{
    auto* impl = TBase::GetThisImpl();
    if (impl->KeyToChild().count(childName) != 0) {
        THROW_ERROR_EXCEPTION(
            NYTree::EErrorCode::AlreadyExists,
            "%v already has a child %Qv",
            impl->GetCapitalizedObjectName(),
            childName);
    }
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::FromNode(
    const INodePtr& node)
{
    auto* result = dynamic_cast<TSelf*>(node.Get());
    YT_ASSERT(result);
    return result;
}

template <class TObject>
TIntrusivePtr<const TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::FromNode(
    const IConstNodePtr& node)
{
    const auto* result = dynamic_cast<const TSelf*>(node.Get());
    YT_ASSERT(result);
    return result;
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectTypeHandlerBase<TObject>>
    TNonversionedMapObjectProxyBase<TObject>::GetTypeHandler() const
{
    auto& objectManager = TBase::Bootstrap_->GetObjectManager();
    auto handler = objectManager->GetHandler(TBase::GetThisImpl());
    auto* mapObjectHandler = static_cast<TTypeHandler*>(handler.Get());
    return mapObjectHandler;
}

template <class TObject>
void TNonversionedMapObjectProxyBase<TObject>::SetImmediateChild(
    TNonversionedMapObjectFactoryBase<TObject>* factory,
    const TYPath& path,
    const TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>>& child)
{
    const auto& [key, _] = PrepareSetChild(
        nullptr,
        path,
        child,
        false /*recursive*/);

    factory->AttachChild(this, key, child);
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::Create(
    EObjectType type,
    const TString& path,
    IAttributeDictionary* attributes)
{
    TObjectProxyBase::DeclareMutating();

    if (type != TBase::GetThisImpl()->GetType()) {
        THROW_ERROR_EXCEPTION("Cannot create an object of type %Qlv, expected type %Qlv",
            type,
            TBase::GetThisImpl()->GetType());
    }
    if (path.empty()) {
        ThrowAlreadyExists(this);
    }

    this->ValidateCreatePermissions(false /*replace*/, attributes);

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
    auto ignoreTypeMismatch = request->ignore_type_mismatch();
    const auto& path = GetRequestTargetYPath(context->RequestHeader());

    IAttributeDictionaryPtr explicitAttributes;
    if (request->has_node_attributes()) {
        explicitAttributes = FromProto(request->node_attributes());
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
    if (ignoreTypeMismatch) {
        THROW_ERROR_EXCEPTION("\"ignore_type_mismatch\" option is not supported for nonversioned map objects");
    }

    context->SetRequestInfo("Type: %v, IgnoreExisting: %v, Recursive: %v, Force: %v, IgnoreTypeMismatch: %v",
        type,
        ignoreExisting,
        recursive,
        force,
        ignoreTypeMismatch);

    auto proxy = Create(type, path, explicitAttributes.Get());
    const auto& objectId = proxy->GetId();

    response->set_cell_tag(ToProto<int>(TBase::Bootstrap_->GetCellTag()));
    ToProto(response->mutable_node_id(), objectId);
    context->SetResponseInfo("ObjectId: %v",
        objectId);

    context->Reply();
}

template <class TObject>
TIntrusivePtr<TNonversionedMapObjectProxyBase<TObject>> TNonversionedMapObjectProxyBase<TObject>::Copy(
    const TString& sourcePath,
    const TString& targetPath,
    ENodeCloneMode mode,
    bool ignoreExisting)
{
    TObjectProxyBase::DeclareMutating();

    auto* impl = TBase::GetThisImpl();
    auto* sourceObject = ResolvePathToNonversionedObject(sourcePath);
    if (sourceObject->GetType() != TBase::GetObject()->GetType()) {
        THROW_ERROR_EXCEPTION("Cannot copy or move an object of type %Qv, expected type %Qv",
            sourceObject->GetType(),
            impl->GetType());
    }

    bool replace = targetPath.empty();
    if (replace) {
        if (!ignoreExisting) {
            ThrowAlreadyExists(this);
        }
        return this;
    }

    auto* sourceImpl = sourceObject->template As<TObject>();
    auto sourceProxy = GetProxy(sourceImpl);

    for (auto* current = impl; current; current = current->GetParent()) {
        if (current == sourceImpl) {
            THROW_ERROR_EXCEPTION("Cannot copy or move an object to its descendant");
        }
    }

    this->ValidateCopyPermissions(sourceImpl, mode, replace, false /*validateAdminister*/);

    auto sourceParent = sourceProxy->DoGetParent();
    if (!sourceParent && mode == ENodeCloneMode::Move) {
        ThrowCannotRemoveNode(sourceProxy);
    }

    auto factory = CreateObjectFactory();

    TSelfPtr clonedProxy;
    if (mode == ENodeCloneMode::Move) {
        factory->DetachChild(sourceParent, sourceProxy);
        clonedProxy = sourceProxy;
    } else {
        auto* clonedObject = factory->CloneObject(sourceImpl, NCypressServer::ENodeCloneMode::Copy);
        clonedProxy = GetProxy(clonedObject);
    }

    SetImmediateChild(factory.get(), targetPath, clonedProxy);
    factory->Commit();

    return clonedProxy;
}

template <class TObject>
DEFINE_YPATH_SERVICE_METHOD(TNonversionedMapObjectProxyBase<TObject>, Copy)
{
    const auto& ypathExt = context->RequestHeader().GetExtension(NYTree::NProto::TYPathHeaderExt::ypath_header_ext);
    if (ypathExt.additional_paths_size() != 1) {
        THROW_ERROR_EXCEPTION("Invalid number of additional paths");
    }
    const auto& sourcePath = ypathExt.additional_paths(0);

    auto mode = CheckedEnumCast<ENodeCloneMode>(request->mode());
    auto recursive = request->recursive();
    auto ignoreExisting = request->ignore_existing();
    auto force = request->force();
    auto targetPath = GetRequestTargetYPath(context->RequestHeader());

    if (recursive) {
        THROW_ERROR_EXCEPTION("\"recursive\" option is not supported for nonversioned map objects");
    }
    if (force) {
        THROW_ERROR_EXCEPTION("\"force\" option is not supported for nonversioned map objects");
    }
    if (ignoreExisting && mode == ENodeCloneMode::Move) {
        THROW_ERROR_EXCEPTION("Cannot specify \"ignore_existing\" for move operation");
    }

    context->SetRequestInfo("SourcePath: %v, Mode: %v, Recursive: %v, IgnoreExisting: %v, Force: %v",
        sourcePath,
        mode,
        recursive,
        ignoreExisting,
        force);

    auto clonedProxy = Copy(sourcePath, targetPath, mode, ignoreExisting);

    ToProto(response->mutable_node_id(), clonedProxy->GetId());

    context->SetResponseInfo("%v: %v",
        targetPath.empty() ? "ExistingObjectId" : "ObjectId",
        clonedProxy->GetId());

    context->Reply();

    if (TBase::Bootstrap_->IsPrimaryMaster()) {
        TBase::ExternalizeToMasters(
            context,
            GetTypeHandler()->GetReplicationCellTags(TBase::GetThisImpl()));
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
TObject* TNonversionedMapObjectFactoryBase<TObject>::CreateObject(IAttributeDictionary* attributes)
{
    auto* object = DoCreateObject(attributes);
    CreatedObjects_.push_back(object);
    return object;
}

template <class TObject>
TObject* TNonversionedMapObjectFactoryBase<TObject>::CloneObject(
    TObject* /*object*/,
    NCypressServer::ENodeCloneMode /*mode*/)
{
    THROW_ERROR_EXCEPTION("Nonversioned map objects do not support cloning");
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::Commit()
{
    for (const auto& event : EventLog_) {
        CommitEvent(event);
    }
    EventLog_.clear();
    CreatedObjects_.clear();
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::Rollback()
{
    for (auto it = EventLog_.rbegin(); it != EventLog_.rend(); ++it) {
        RollbackEvent(*it);
    }
    EventLog_.clear();
    RemoveCreatedObjects();
    CreatedObjects_.clear();
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
    try {
        parent->ValidateBeforeAttachChild(key, child);

        parent->AttachChild(key, child);
        LogEvent({
            EEventType::AttachChild,
            parent,
            key,
            child
        });

        parent->ValidateAfterAttachChild(key, child);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            "Failed to attach child %Qv to %v",
            key,
            parent->GetObject()->template As<TObject>()->GetLowercaseObjectName())
            << ex;
    }
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::DetachChild(const TProxyPtr& parent, const TProxyPtr& child)
{
    Bootstrap_->GetObjectManager()->RefObject(parent->GetObject());
    LogEvent({
        EEventType::RefObject,
        parent,
        TString(), /*key*/
        child /*child*/
    });

    auto key = parent->GetChildKeyOrThrow(child);
    parent->DetachChild(child);
    LogEvent({
        EEventType::DetachChild,
        parent,
        key,
        child
    });
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::RemoveCreatedObjects()
{
    FlushObjectUnrefs();
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* object : CreatedObjects_) {
        YT_VERIFY(object->GetObjectRefCounter() == 1);
        objectManager->UnrefObject(object);
    }
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::CommitEvent(const TFactoryEvent& event)
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    if (event.Type == EEventType::RefObject) {
        objectManager->UnrefObject(event.Parent->GetObject());
    }
}

template <class TObject>
void TNonversionedMapObjectFactoryBase<TObject>::RollbackEvent(const TFactoryEvent& event)
{
    try {
        switch (event.Type) {
            case EEventType::RefObject:
                Bootstrap_->GetObjectManager()->UnrefObject(event.Parent->GetObject());
                break;
            case EEventType::AttachChild:
                event.Parent->DetachChild(event.Child);
                break;
            case EEventType::DetachChild:
                event.Parent->AttachChild(event.Key, event.Child);
                break;
            default:
                Y_UNREACHABLE();
        }
    } catch (const std::exception& ex) {
        const auto& Logger = NObjectServer::ObjectServerLogger;
        YT_LOG_FATAL(ex, "Unhandled exception during rollback of factory event (EventType: %v)", event.Type);
    }
}

////////////////////////////////////////////////////////////////////////////////

template class TNonversionedMapObjectProxyBase<NSecurityServer::TAccount>;
template class TNonversionedMapObjectFactoryBase<NSecurityServer::TAccount>;

template class TNonversionedMapObjectProxyBase<NSchedulerPoolServer::TSchedulerPool>;
template class TNonversionedMapObjectFactoryBase<NSchedulerPoolServer::TSchedulerPool>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
