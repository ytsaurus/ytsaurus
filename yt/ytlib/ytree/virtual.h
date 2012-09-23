#pragma once

#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TSupportsAttributes
    , public ISystemAttributeProvider
{
protected:
    virtual std::vector<Stroka> GetKeys(size_t sizeLimit = Max<size_t>()) const = 0;
    virtual size_t GetSize() const = 0;
    virtual IYPathServicePtr GetItemService(const TStringBuf& key) const = 0;

private:
    virtual void DoInvoke(NRpc::IServiceContextPtr context);

    virtual TResolveResult ResolveRecursive(const TYPath& path, NRpc::IServiceContextPtr context);
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context);
    virtual void ListSelf(TReqList* request, TRspList* response, TCtxList* context);

    // TSupportsAttributes overrides
    virtual ISystemAttributeProvider* GetSystemAttributeProvider();

    // ISystemAttributeProvider overrides
    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer);
    virtual bool SetSystemAttribute(const Stroka& key, const TYsonString& value);
};

////////////////////////////////////////////////////////////////////////////////

INodePtr CreateVirtualNode(IYPathServicePtr service);
INodePtr CreateVirtualNode(TYPathServiceProducer producer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
