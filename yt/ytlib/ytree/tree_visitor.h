#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Traverses a YTree and invokes appropriate methods of IYsonConsumer.
class TTreeVisitor
    : private TNonCopyable
{
public:
    //! Initializes an instance.
    /*!
     *  \param consumer A consumer to call.
     *  \param visitAttributes Enables going into attribute maps during traversal.
     */
    TTreeVisitor(IYsonConsumer* consumer, bool visitAttributes = true);

    //! Starts the traversal.
    /*!
     *  \param root A root from which to start.
     */
    void Visit(INode::TPtr root);

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

    void VisitAny(INode::TPtr node);
    void VisitScalar(INode::TPtr node, bool hasAttributes);
    void VisitEntity(INode::TPtr node, bool hasAttributes);
    void VisitList(IListNode::TPtr node, bool hasAttributes);
    void VisitMap(IMapNode::TPtr node, bool hasAttributes);
    void VisitAttributes(IMapNode::TPtr node);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
