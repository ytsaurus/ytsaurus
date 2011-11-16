#pragma once

#include "common.h"
#include "node.h"

#include "../ytree/ytree.h"
#include "../ytree/ypath_service.h"

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

    virtual INodeTypeHandler::TPtr GetTypeHandler() const = 0;

    virtual bool IsOperationLogged(NYTree::TYPath path, const Stroka& verb) const = 0;;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT
