#pragma once

#include "interned_attributes.h"
#include "system_attribute_provider.h"
#include "ypath_detail.h"

#include <yt/core/yson/producer.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TSupportsAttributes
    , public ISystemAttributeProvider
{
protected:
    explicit TVirtualMapBase(INodePtr owningNode = nullptr);

    virtual std::vector<TString> GetKeys(i64 limit = std::numeric_limits<i64>::max()) const = 0;
    virtual i64 GetSize() const = 0;

    virtual IYPathServicePtr FindItemService(TStringBuf key) const = 0;
    virtual void OnRecurse(const NRpc::IServiceContextPtr& context, TStringBuf key) const;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual TResolveResult ResolveRecursive(const TYPath& path, const NRpc::IServiceContextPtr& context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;
    virtual void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override;

    // TSupportsAttributes overrides
    virtual ISystemAttributeProvider* GetBuiltinAttributeProvider() override;

    // ISystemAttributeProvider overrides
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual const THashSet<TInternedAttributeKey>& GetBuiltinAttributeKeys() override;
    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;
    virtual TFuture<NYson::TYsonString> GetBuiltinAttributeAsync(TInternedAttributeKey key) override;
    virtual bool SetBuiltinAttribute(TInternedAttributeKey key, const NYson::TYsonString& value) override;
    virtual bool RemoveBuiltinAttribute(TInternedAttributeKey key) override;

private:
    const INodePtr OwningNode_;

    TBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;

};

////////////////////////////////////////////////////////////////////////////////

class TCompositeMapService
    : public TVirtualMapBase
{
public:
    TCompositeMapService();

    virtual std::vector<TString> GetKeys(i64 limit = std::numeric_limits<i64>::max()) const override;
    virtual i64 GetSize() const override;
    virtual IYPathServicePtr FindItemService(TStringBuf key) const override;
    virtual void ListSystemAttributes(std::vector<TAttributeDescriptor>* descriptors) override;
    virtual bool GetBuiltinAttribute(TInternedAttributeKey key, NYson::IYsonConsumer* consumer) override;

    TIntrusivePtr<TCompositeMapService> AddChild(const TString& key, IYPathServicePtr service);
    TIntrusivePtr<TCompositeMapService> AddAttribute(TInternedAttributeKey key, NYson::TYsonCallback producer);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

INodePtr CreateVirtualNode(IYPathServicePtr service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
