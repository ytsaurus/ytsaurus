#pragma once

#include "type_handler.h"

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/master/cypress_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/core/ytree/ypath_detail.h>
#include <yt/yt/core/ytree/system_attribute_provider.h>
#include <yt/yt/core/ytree/virtual.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ypath/public.h>

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

class TVirtualSinglecellMapBase
    : public NYTree::TVirtualMapBase
{
protected:
    NCellMaster::TBootstrap* const Bootstrap_;

    TVirtualSinglecellMapBase(
        NCellMaster::TBootstrap* bootstrap,
        NYTree::INodePtr owningNode = nullptr);

private:
    std::optional<NYTree::TVirtualCompositeNodeReadOffloadParams> GetReadOffloadParams() const override;
};

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
        NYTree::INodePtr owningNode,
        bool ignoreForeignObjects = false);

    virtual TFuture<std::vector<NObjectClient::TObjectId>> GetKeys(i64 sizeLimit = std::numeric_limits<i64>::max()) const = 0;
    virtual TFuture<i64> GetSize() const = 0;
    virtual bool IsValid(NObjectServer::TObject* object) const = 0;
    virtual NYPath::TYPath GetWellKnownPath() const = 0;

    bool DoInvoke(const NYTree::IYPathServiceContextPtr& context) override;

    TResolveResult ResolveRecursive(const NYPath::TYPath& path, const NYTree::IYPathServiceContextPtr& context) override;
    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override;

    // TSupportsAttributes overrides
    NYTree::ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // ISystemAttributeProvider overrides
    void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    const THashSet<NYTree::TInternedAttributeKey>& GetBuiltinAttributeKeys() override;
    bool GetBuiltinAttribute(NYTree::TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(NYTree::TInternedAttributeKey key) override;
    bool SetBuiltinAttribute(NYTree::TInternedAttributeKey key, const NYson::TYsonString& value, bool force) override;
    bool RemoveBuiltinAttribute(NYTree::TInternedAttributeKey key) override;

    virtual bool NeedSuppressUpstreamSync() const;
    virtual bool NeedSuppressTransactionCoordinatorSync() const;

    virtual TFuture<std::vector<std::pair<NObjectClient::TCellTag, i64>>> FetchSizes();

private:
    bool IgnoreForeignObjects_ = false;

    NYTree::TSystemBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;

    struct TFetchItem
    {
        TString Key;
        NYson::TYsonString Attributes;
    };

    struct TFetchItemsSession
        : public TRefCounted
    {
        IInvokerPtr Invoker;
        i64 Limit = -1;
        NYTree::TAttributeFilter AttributeFilter;
        bool Incomplete = false;
        std::vector<TFetchItem> Items;
    };

    using TFetchItemsSessionPtr = TIntrusivePtr<TFetchItemsSession>;

    TFuture<TFetchItemsSessionPtr> FetchItems(
        i64 limit,
        const NYTree::TAttributeFilter& attributeFilter);

    TFuture<void> FetchItemsFromLocal(const TFetchItemsSessionPtr& session);
    TFuture<void> FetchItemsFromRemote(const TFetchItemsSessionPtr& session, NObjectClient::TCellTag cellTag);

    TFuture<std::pair<NObjectClient::TCellTag, i64>> FetchSizeFromLocal();
    TFuture<std::pair<NObjectClient::TCellTag, i64>> FetchSizeFromRemote(NObjectClient::TCellTag cellTag);

    TFuture<NYson::TYsonString> GetOwningNodeAttributes(const NYTree::TAttributeFilter& attributeFilter);

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

} // namespace NYT::NCypressServer

#define VIRTUAL_INL_H_
#include "virtual-inl.h"
#undef VIRTUAL_INL_H_
