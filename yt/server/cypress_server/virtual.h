#pragma once

#include "type_handler.h"

#include <core/misc/nullable.h>

#include <core/ytree/ypath_detail.h>
#include <core/ytree/system_attribute_provider.h>
#include <core/ytree/yson_string.h>

#include <core/ypath/public.h>

#include <server/hydra/entity_map.h>

#include <server/object_server/public.h>

#include <server/cypress_server/public.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMulticellMapBase
    : public NYTree::TSupportsAttributes
    , public NYTree::ISystemAttributeProvider
{
protected:
    NCellMaster::TBootstrap* const Bootstrap_;
    const NYTree::INodePtr OwningNode_;


    TVirtualMulticellMapBase(
        NCellMaster::TBootstrap* bootstrap,
        NYTree::INodePtr owningNode);

    virtual std::vector<NObjectClient::TObjectId> GetKeys(i64 sizeLimit = std::numeric_limits<i64>::max()) const = 0;
    virtual i64 GetSize() const = 0;
    virtual bool IsValid(NObjectServer::TObjectBase* object) const = 0;
    virtual NYPath::TYPath GetWellKnownPath() const = 0;

    virtual bool DoInvoke(NRpc::IServiceContextPtr context) override;

    virtual TResolveResult ResolveRecursive(const NYPath::TYPath& path, NRpc::IServiceContextPtr context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGetPtr context) override;
    virtual void ListSelf(TReqList* request, TRspList* response, TCtxListPtr context) override;

    // TSupportsAttributes overrides
    virtual NYTree::ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // ISystemAttributeProvider overrides
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(const Stroka& key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYTree::TYsonString> GetBuiltinAttributeAsync(const Stroka& key) override;
    virtual bool SetBuiltinAttribute(const Stroka& key, const NYTree::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(const Stroka& key) override;

private:
    TFuture<std::vector<std::pair<NObjectClient::TCellTag, i64>>> FetchSizes();

    struct TFetchItem
    {
        Stroka Key;
        TNullable<NYTree::TYsonString> Attributes;
    };

    struct TFetchItemsSession
        : public TIntrinsicRefCounted
    {
        i64 Limit = -1;
        NYTree::TAttributeFilter AttributeFilter;
        std::vector<NObjectClient::TCellTag> CellTags;
        int CellTagIndex = 0;
        bool Incomplete = false;
        std::vector<TFetchItem> Items;
    };

    using TFetchItemsSessionPtr = TIntrusivePtr<TFetchItemsSession>;

    TFuture<TFetchItemsSessionPtr> FetchItems(
        i64 limit,
        const NYTree::TAttributeFilter& attributeFilter);

    void FetchItemsFromLocal(
        TFetchItemsSessionPtr session,
        TPromise<TFetchItemsSessionPtr> promise);

    void FetchItemsFromRemote(
        TFetchItemsSessionPtr session,
        TPromise<TFetchItemsSessionPtr> promise);

    NYTree::TYsonString GetOwningNodeAttributes(const NYTree::TAttributeFilter& attributeFilter);

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Enumerate);

};

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM(EVirtualNodeOptions,
    ((None)            (0x0000))
    ((RequireLeader)   (0x0001))
    ((RedirectSelf)    (0x0002))
);

using TYPathServiceProducer = TCallback<NYTree::IYPathServicePtr(NYTree::INodePtr owningNode)>;

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType,
    TYPathServiceProducer producer,
    EVirtualNodeOptions options);

template <class TId, class TValue>
NYTree::IYPathServicePtr CreateVirtualObjectMap(
    NCellMaster::TBootstrap* bootstrap,
    const NHydra::TReadOnlyEntityMap<TId, TValue>& map,
    NYTree::INodePtr owningNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

#define VIRTUAL_INL_H_
#include "virtual-inl.h"
#undef VIRTUAL_INL_H_
