#pragma once

#ifndef MAP_OBJECT_TYPE_HANDLER_INL_H_
#error "Direct inclusion of this file is not allowed, include map_object.h"
// For the sake of sane code completion.
#include "map_object_type_handler.h"
#endif

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

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
        THROW_ERROR_EXCEPTION("Cannot add a child to %Qv")
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
NObjectServer::TObject* TNonversionedMapObjectTypeHandlerBase<TObject>::DoCreateObject(
    const TString& name,
    TObject* parent,
    NYTree::IAttributeDictionary* attributes)
{
    auto ancestorProxy = TBase::GetProxy(parent, nullptr);

    auto req = NCypressClient::TCypressYPathProxy::Create("/" + name);
    req->set_type(static_cast<int>(this->GetType()));
    ToProto(req->mutable_node_attributes(), *attributes);
    auto rsp = NYTree::SyncExecuteVerb(ancestorProxy, req);
    auto objectId = FromProto<TObjectId>(rsp->node_id());

    return TBase::FindObject(objectId);
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
        THROW_ERROR_EXCEPTION("Depth limit exceeded");
    }
    for (const auto& [child, _] : root->ChildToKey()) {
        ValidateHeightLimit(child, heightLimit - 1);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
