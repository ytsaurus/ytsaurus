#pragma once

#include "system_attribute_provider.h"
#include "ypath_detail.h"

#include <yt/core/yson/producer.h>

namespace NYT::NYTree {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TSupportsAttributes
    , public ISystemAttributeProvider
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque, true);

protected:
    explicit TVirtualMapBase(INodePtr owningNode = nullptr);

    virtual std::vector<TString> GetKeys(i64 limit = std::numeric_limits<i64>::max()) const = 0;
    virtual i64 GetSize() const = 0;

    virtual IYPathServicePtr FindItemService(TStringBuf key) const = 0;

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

class TVirtualListBase
    : public TSupportsAttributes
    , public ISystemAttributeProvider
{
public:
    DEFINE_BYVAL_RW_PROPERTY(bool, Opaque, true);

protected:
    virtual i64 GetSize() const = 0;
    virtual IYPathServicePtr FindItemService(int index) const = 0;

    virtual bool DoInvoke(const NRpc::IServiceContextPtr& context) override;

    virtual TResolveResult ResolveRecursive(const TYPath& path, const NRpc::IServiceContextPtr& context) override;
    virtual void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override;

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
    TBuiltinAttributeKeysCache BuiltinAttributeKeysCache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree

#define VIRTUAL_INL_H_
#include "virtual-inl.h"
#undef VIRTUAL_INL_H_
