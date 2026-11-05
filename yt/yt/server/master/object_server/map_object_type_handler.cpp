#include "map_object_type_handler.h"

#include "helpers.h"
#include "map_object_proxy.h"

#include <yt/yt/server/master/security_server/account.h>

#include <yt/yt/server/master/scheduler_pool_server/scheduler_pool.h>

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
    auto parent = object->GetParent();
    return parent ? parent : TObjectTypeHandlerWithMapBase<TObject>::DoGetParent(object);
}

template <class TObject>
void TNonversionedMapObjectTypeHandlerBase<TObject>::ValidateObjectName(const std::string& name)
{
    NObjectServer::ValidateObjectName(name, this->GetType(), MaxNameLength_);
}

template <class TObject>
IObjectProxyPtr TNonversionedMapObjectTypeHandlerBase<TObject>::DoGetProxy(
    TObject* object,
    NTransactionServer::TTransaction* /*transaction*/)
{
    return GetMapObjectProxy(object);
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

    if (auto parent = object->GetParent()) {
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
    const std::string& name,
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

template <class TObject>
std::optional<int> TNonversionedMapObjectTypeHandlerBase<TObject>::GetSubtreeSizeLimit() const
{
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

template class TNonversionedMapObjectTypeHandlerBase<NSecurityServer::TAccount>;
template class TNonversionedMapObjectTypeHandlerBase<NSchedulerPoolServer::TSchedulerPool>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
