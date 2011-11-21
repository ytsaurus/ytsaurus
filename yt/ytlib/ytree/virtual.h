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

    RPC_SERVICE_METHOD_DECL(NProto, Get);

};

////////////////////////////////////////////////////////////////////////////////

INode::TPtr CreateVirtualNode(
    TYPathServiceProducer* producer,
    INodeFactory* factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
