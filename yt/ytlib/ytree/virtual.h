#pragma once

#include "common.h"
#include "ypath_detail.h"
#include "ypath_rpc.pb.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TVirtualMapBase
    : public TYPathServiceBase
{
protected:
    virtual yvector<Stroka> GetKeys() = 0;
    virtual IYPathService::TPtr GetItemService(const Stroka& key) = 0;

private:
    virtual TResolveResult ResolveRecursive(TYPath path, const Stroka& verb);
    virtual void DoInvoke(NRpc::IServiceContext* context);

    DECLARE_RPC_SERVICE_METHOD(NProto, Get);

};

////////////////////////////////////////////////////////////////////////////////

INode::TPtr CreateVirtualNode(
    TYPathServiceProvider* provider,
    INodeFactory* factory);

INode::TPtr CreateVirtualNode(
    IYPathService* service,
    INodeFactory* factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
