#include "map_object_type_handler.h"
#include "map_object_proxy.h"

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

template <class TObject>
TNonversionedMapObjectTypeHandlerBase<TObject>::TNonversionedMapObjectTypeHandlerBase(
    NCellMaster::TBootstrap* bootstrap,
    TMapType* map)
    : TBase(bootstrap, map)
{ }

template <class TObject>
ETypeFlags TNonversionedMapObjectTypeHandlerBase<TObject>::GetFlags() const
{
    return
        ETypeFlags::Creatable |
        ETypeFlags::Removable;
}

template <class TObject>
std::unique_ptr<NObjectServer::TObject>
TNonversionedMapObjectTypeHandlerBase<TObject>::InstantiateObject(TObjectId id)
{
    return std::make_unique<TObject>(id);
}

template <class TObject>
NObjectServer::TObject* TNonversionedMapObjectTypeHandlerBase<TObject>::DoGetParent(TObject* object)
{
    auto* parent = object->GetParent();
    return parent ? parent : TObjectTypeHandlerBase<TObject>::DoGetParent(object);
}

template <class TObject>
void TNonversionedMapObjectTypeHandlerBase<TObject>::ValidateObjectName(const TString& name)
{
    if (name.empty()) {
        THROW_ERROR_EXCEPTION("Name cannot be empty");
    }
    if (name.find("/") != TString::npos) {
        THROW_ERROR_EXCEPTION("Name cannot contain slashes");
    }
    if (name.StartsWith(NObjectClient::ObjectIdPathPrefix)) {
        THROW_ERROR_EXCEPTION("Name cannot start with %Qv",
            NObjectClient::ObjectIdPathPrefix);
    }
}

template <class TObject>
void TNonversionedMapObjectTypeHandlerBase<TObject>::ValidateAttachChildDepth(
    const TObject* parent, const TObject* child)
{
    auto depthLimit = GetDepthLimit();
    if (!depthLimit) {
        return;
    }

    auto heightLimit = *depthLimit - GetDepth(parent) - 1;
    try {
        ValidateHeightLimit(child, heightLimit);
    } catch (const std::exception& ex) {
        // XXX(kiselyovp) object name is capitalized here, fix this in YT-11362
        THROW_ERROR_EXCEPTION("Cannot add a child to %v", parent->GetObjectName())
            << ex;
    }
}

template <class TObject>
TString TNonversionedMapObjectTypeHandlerBase<TObject>::DoGetName(const TObject* object)
{
    return Format("object %v", object->GetName());
}

template <class TObject>
NSecurityServer::TAccessControlDescriptor*
TNonversionedMapObjectTypeHandlerBase<TObject>::DoFindAcd(TObject* object)
{
    return &object->Acd();
}

template <class TObject>
void TNonversionedMapObjectTypeHandlerBase<TObject>::DoZombifyObject(TObject* object)
{
    YT_VERIFY(object->KeyToChild().empty());
    YT_VERIFY(object->ChildToKey().empty());

    auto* parent = object->GetParent();
    if (parent) {
        auto name = object->GetName();
        UnregisterName(name, object);
        parent->DetachChild(object);

        const auto& objectManager = TBase::Bootstrap_->GetObjectManager();
        objectManager->UnrefObject(parent);
    }

    TBase::DoZombifyObject(object);
}

template <class TObject>
NObjectServer::TObject* TNonversionedMapObjectTypeHandlerBase<TObject>::CreateObjectImpl(
    const TString& name,
    TObject* parent,
    NYTree::IAttributeDictionary* attributes)
{
    auto ancestorProxy = TNonversionedMapObjectProxyBase<TObject>::GetProxy(
        TBase::Bootstrap_,
        parent);
    ancestorProxy->ValidateChildName(name);

    auto objectProxy = ancestorProxy->Create(this->GetType(), "/" + name, attributes);
    return objectProxy->GetObject()->template As<TObject>();
}

template <class TObject>
std::optional<int> TNonversionedMapObjectTypeHandlerBase<TObject>::GetDepthLimit() const
{
    return std::nullopt;
}

template <class TObject>
int TNonversionedMapObjectTypeHandlerBase<TObject>::GetDepth(const TObject* object) const
{
    YT_VERIFY(object);
    auto depth = 0;
    for (auto* current = object->GetParent(); current; current = current->GetParent()) {
        ++depth;
    }

    return depth;
}

template <class TObject>
void TNonversionedMapObjectTypeHandlerBase<TObject>::ValidateHeightLimit(
    const TObject* root,
    int heightLimit) const
{
    YT_VERIFY(root);
    if (heightLimit < 0) {
        THROW_ERROR_EXCEPTION("Tree height limit exceeded");
    }
    for (const auto& [child, _] : root->ChildToKey()) {
        ValidateHeightLimit(child, heightLimit - 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
