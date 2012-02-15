#pragma once

#include "public.h"

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
    void Visit(INode* root);

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

    void VisitAny(INode* node);
    void VisitScalar(INode* node, bool hasAttributes);
    void VisitEntity(INode* node, bool hasAttributes);
    void VisitList(IListNode* node, bool hasAttributes);
    void VisitMap(IMapNode* node, bool hasAttributes);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
