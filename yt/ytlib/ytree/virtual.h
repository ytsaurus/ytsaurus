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
{
protected:
    virtual TResolveResult ResolveAttributes(const NYTree::TYPath& path, const Stroka& verb);

    virtual void DoInvoke(NRpc::IServiceContext* context);
    
    virtual void GetSystemAttributes(yvector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer);
   
    virtual void GetAttribute(const NYTree::TYPath& path, TReqGet* request, TRspGet* response, TCtxGet* context);
    virtual void ListAttribute(const NYTree::TYPath& path, TReqList* request, TRspList* response, TCtxList* context);
};

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TAttributedYPathServiceBase
{
protected:
    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const = 0;
    virtual size_t GetSize() const = 0;
    virtual IYPathService::TPtr GetItemService(const Stroka& key) const = 0;

private:
    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb);
    virtual void GetSelf(TReqGet* request, TRspGet* response, TCtxGet* context);

    virtual void GetSystemAttributes(yvector<TAttributeInfo>* attributes);
    virtual bool GetSystemAttribute(const Stroka& name, IYsonConsumer* consumer);
};

////////////////////////////////////////////////////////////////////////////////

INode::TPtr CreateVirtualNode(TYPathServiceProvider* provider);
INode::TPtr CreateVirtualNode(IYPathService* service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
