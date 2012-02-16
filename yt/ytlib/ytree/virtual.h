#pragma once

#include "common.h"
#include "ypath_detail.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TAttributedYPathServiceBase
    : public TYPathServiceBase
    , public TSupportsGet
    , public TSupportsList
    , public ISystemAttributeProvider
{
protected:
    virtual TResolveResult ResolveAttributes(const NYTree::TYPath& path, const Stroka& verb);

    virtual void DoInvoke(NRpc::IServiceContext* context);
    
    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer);
    virtual bool SetSystemAttribute(const Stroka& key, TYsonProducer producer);
   
    // TODO(roizner): Use mix-in TSupportsAttributes
    virtual void GetAttribute(const NYTree::TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context);
    virtual void ListAttribute(const NYTree::TYPath& path, TReqList* request, TRspList* response, TCtxList* context);
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TAttributedYPathServiceBase
{
protected:
    virtual yvector<Stroka> GetKeys(size_t sizeLimit = Max<size_t>()) const = 0;
    virtual size_t GetSize() const = 0;
    virtual IYPathServicePtr GetItemService(const Stroka& key) const = 0;

private:
    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb);
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context);
    virtual void ListSelf(TReqList* request, TRspList* response, TCtxList* context);

    virtual void GetSystemAttributes(std::vector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& key, IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

INodePtr CreateVirtualNode(TYPathServiceProvider* provider);
INodePtr CreateVirtualNode(IYPathService* service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
