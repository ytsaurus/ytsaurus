#pragma once

#include "common.h"
#include "node.h"

namespace NYT {
namespace NCypress {

// TODO: rename to dynamic_type.h

////////////////////////////////////////////////////////////////////////////////

struct IDynamicTypeHandler
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IDynamicTypeHandler> TPtr;

    virtual ERuntimeNodeType GetRuntimeType() = 0;
    virtual Stroka GetTypeName() = 0;
    
    virtual TAutoPtr<ICypressNode> Create(
        const TNodeId& nodeId,
        const TTransactionId& transactionId,
        IMapNode::TPtr description) = 0;

    virtual TAutoPtr<ICypressNode> Create(
        const TNodeId& nodeId,
        const TTransactionId& transactionId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
