#include "map_object_type_handler.h"
#include "map_object_proxy.h"

#include <yt/server/master/scheduler_pool_server/scheduler_pool.h>

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
    if (name.StartsWith(NObjectClient::ObjectIdPathPrefix)) {
        THROW_ERROR_EXCEPTION("Name cannot start with %Qv",
            NObjectClient::ObjectIdPathPrefix);
    }
    for (auto ch : name) {
        if (NYPath::IsSpecialCharacter(ch)) {
            THROW_ERROR_EXCEPTION("Name cannot contain %Qv symbol",
                ch);
        }
    }
}

template <class TObject>
IObjectProxyPtr TNonversionedMapObjectTypeHandlerBase<TObject>::DoGetProxy(
    TObject* object,
    NTransactionServer::TTransaction* /* transaction */)
{
    return GetMapObjectProxy(object);
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
    ValidateObjectName(name);
    auto ancestorProxy = GetMapObjectProxy(parent);
    auto objectProxy = ancestorProxy->Create(this->GetType(), "/" + name, attributes);
    return objectProxy->GetObject();
}

template <class TObject>
std::optional<int> TNonversionedMapObjectTypeHandlerBase<TObject>::GetDepthLimit() const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

template class TNonversionedMapObjectTypeHandlerBase<NSchedulerPoolServer::TSchedulerPool>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
