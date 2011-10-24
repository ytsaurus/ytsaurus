#pragma once

#include "common.h"

#include "../cypress/node_type.h"

namespace NYT {
namespace NFileServer {

using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

class TFileTypeHandler
    : public IDynamicTypeHandler
{
public:
    virtual ERuntimeNodeType GetRuntimeType();

    virtual Stroka GetTypeName();
    
    virtual TAutoPtr<ICypressNode> Create(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        IMapNode::TPtr description);

    virtual TAutoPtr<ICypressNode> Load(TInputStream* stream);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

