#pragma once

#include "common.h"

#include "../cypress/node.h"

namespace NYT {
namespace NFileServer {

using namespace NCypress;

////////////////////////////////////////////////////////////////////////////////

class TFileNode
    : public TCypressNodeBase
{
public:
    explicit TFileNode(const TBranchedNodeId& id);

    virtual TAutoPtr<ICypressNode> Branch(
        TIntrusivePtr<TCypressManager> cypressManager,
        const TTransactionId& transactionId) const;
    virtual void Merge(
        TIntrusivePtr<TCypressManager> cypressManager,
        ICypressNode& branchedNode);

    virtual TAutoPtr<ICypressNode> Clone() const;

    virtual ERuntimeNodeType GetRuntimeType() const
    {
        return ERuntimeNodeType::File;
    }

    virtual TIntrusivePtr<ICypressNodeProxy> GetProxy(
        TIntrusivePtr<TCypressManager> state,
        const TTransactionId& transactionId) const;

    virtual void Destroy(TIntrusivePtr<TCypressManager> cypressManager);

private:
    typedef TFileNode TThis;

    TFileNode(const TBranchedNodeId& id, const TFileNode& other);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFileServer
} // namespace NYT

