#pragma once

#include "common.h"
#include "file_node.h"

#include "../cypress/node_proxy_detail.h"

namespace NYT {
namespace NFileServer {

////////////////////////////////////////////////////////////////////////////////

class TFileNodeProxy
    : public TCypressNodeProxyBase<IEntityNode, TFileNode>
{
public:
    typedef TIntrusivePtr<TFileNodeProxy> TPtr;

    TFileNodeProxy(
        TCypressManager::TPtr cypressManager,
        const TTransactionId& transactionId,
        const TNodeId& nodeId);

    virtual ENodeType GetType() const;
    virtual Stroka GetTypeName() const;

protected:
    virtual IAttributeProvider* GetAttributeProvider();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

