#pragma once

#include "common.h"
#include "node.h"

#include "../ytree/ytree.h"

namespace NYT {
namespace NCypress {

////////////////////////////////////////////////////////////////////////////////

struct ICypressNodeProxy
    : virtual NYTree::INode
{
    typedef TIntrusivePtr<ICypressNodeProxy> TPtr;

    // TODO: removing this causes link error, investigate!
    ICypressNodeProxy()
    { }

    virtual TTransactionId GetTransactionId() const = 0;
    virtual TNodeId GetNodeId() const = 0;

    virtual const ICypressNode& GetImpl() const = 0;
    virtual ICypressNode& GetImplForUpdate() = 0;

    virtual Stroka GetTypeName() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
