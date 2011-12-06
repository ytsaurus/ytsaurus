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
    void Visit(const INode* root);

private:
    IYsonConsumer* Consumer;
    bool VisitAttributes_;

    void VisitAny(const INode* node);
    void VisitScalar(const INode* node, bool hasAttributes);
    void VisitEntity(const INode* node, bool hasAttributes);
    void VisitList(const IListNode* node, bool hasAttributes);
    void VisitMap(const IMapNode* node, bool hasAttributes);
    void VisitAttributes(const IMapNode* node);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
