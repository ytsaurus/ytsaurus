#pragma once

#include "common.h"
#include "ypath_detail.h"
#include "ypath.pb.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TYPathServiceBase
{
protected:
    virtual yvector<Stroka> GetKeys(size_t sizeLimit) const = 0;
    virtual size_t GetSize() const = 0;
    virtual IYPathService::TPtr GetItemService(const Stroka& key) const = 0;

private:
    virtual TResolveResult ResolveRecursive(const TYPath& path, const Stroka& verb);
    virtual void DoInvoke(NRpc::IServiceContext* context);

    DECLARE_RPC_SERVICE_METHOD(NProto, Get);

};

////////////////////////////////////////////////////////////////////////////////

INode::TPtr CreateVirtualNode(TYPathServiceProvider* provider);

INode::TPtr CreateVirtualNode(IYPathService* service);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
