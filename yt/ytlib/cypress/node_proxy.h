#pragma once

#include "common.h"
#include "node.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy
    : virtual NYTree::INode
{
    typedef TIntrusivePtr<ICypressNodeProxy> TPtr;

    // TODO: kill me
    ICypressNodeProxy() {}

    virtual TTransactionId GetTransactionId() const = 0;
    virtual TNodeId GetNodeId() const = 0;

    virtual const ICypressNode& GetImpl() const = 0;
    virtual ICypressNode& GetImplForUpdate() = 0;

    virtual Stroka GetTypeName() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
