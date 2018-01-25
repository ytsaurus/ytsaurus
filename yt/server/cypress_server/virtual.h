#pragma once

#include "type_handler.h"

#include <yt/server/hydra/entity_map.h>

#include <yt/server/object_server/public.h>

#include <yt/server/cypress_server/public.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/server/object_server/public.h>

#include <yt/core/misc/nullable.h>

#include <yt/core/ytree/ypath_detail.h>
#include <yt/core/ytree/system_attribute_provider.h>

#include <yt/core/yson/string.h>

#include <yt/core/ypath/public.h>

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

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual TResolveResult ResolveRecursive(const NYPath::TYPath& path, const NRpc::IServiceContextPtr& context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    virtual void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override;

    // TSupportsAttributes overrides
    virtual NYTree::ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // ISystemAttributeProvider overrides
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual const yhash_set<const char*>& GetBuiltinAttributeKeys() override;
    virtual bool GetBuiltinAttribute(const TString& key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(const TString& key) override;
    virtual bool SetBuiltinAttribute(const TString& key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(const TString& key) override;

private:
    NYTree::TBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;

    TFuture<std::vector<std::pair<NObjectClient::TCellTag, i64>>> FetchSizes();

    struct TFetchItem
    {
        TString Key;
        NYson::TYsonString Attributes;
    };

    struct TFetchItemsSession
        : public TIntrinsicRefCounted
    {
        IInvokerPtr Invoker;
        i64 Limit = -1;
        TNullable<std::vector<TString>> AttributeKeys;
        bool Incomplete = false;
        std::vector<TFetchItem> Items;
    };

    using TFetchItemsSessionPtr = TIntrusivePtr<TFetchItemsSession>;

    TFuture<TFetchItemsSessionPtr> FetchItems(
        i64 limit,
        const TNullable<std::vector<TString>>& attributeKeys);

    TFuture<void> FetchItemsFromLocal(const TFetchItemsSessionPtr& session);
    TFuture<void> FetchItemsFromRemote(const TFetchItemsSessionPtr& session, NObjectClient::TCellTag cellTag);

    TFuture<std::pair<NObjectClient::TCellTag, i64>> FetchSizeFromLocal();
    TFuture<std::pair<NObjectClient::TCellTag, i64>> FetchSizeFromRemote(NObjectClient::TCellTag cellTag);

    TFuture<NYson::TYsonString> GetOwningNodeAttributes(const TNullable<std::vector<TString>>& attributeKeys);

    DECLARE_YPATH_SERVICE_METHOD(NCypressClient::NProto, Enumerate);

};

////////////////////////////////////////////////////////////////////////////////

DEFINE_BIT_ENUM(EVirtualNodeOptions,
    ((None)            (0x0000))
    ((RedirectSelf)    (0x0001))
);

using TYPathServiceProducer = TCallback<NYTree::IYPathServicePtr(NYTree::INodePtr owningNode)>;

INodeTypeHandlerPtr CreateVirtualTypeHandler(
    NCellMaster::TBootstrap* bootstrap,
    NObjectClient::EObjectType objectType,
    TYPathServiceProducer producer,
    EVirtualNodeOptions options);

template <class TValue>
NYTree::IYPathServicePtr CreateVirtualObjectMap(
    NCellMaster::TBootstrap* bootstrap,
    const NHydra::TReadOnlyEntityMap<TValue>& map,
    NYTree::INodePtr owningNode);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT

#define VIRTUAL_INL_H_
#include "virtual-inl.h"
#undef VIRTUAL_INL_H_
