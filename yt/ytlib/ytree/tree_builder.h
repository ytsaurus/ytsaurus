#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Reconstructs a YTree from IYsonConsumer calls.
struct ITreeBuilder
    : public virtual IYsonConsumer
{
    //! Resets the instance.
    virtual void BeginTree() = 0;

    //! Returns the root node of the constructed tree.
    /*!
     *  \note
     *  Must be called after the tree is constructed.
     */
    virtual INode::TPtr EndTree() = 0;


    // TODO: document
    virtual void OnNode(INode* node) = 0;
};

//! Creates a builder that makes explicit calls to the factory.
/*!
 *  \param factory A factory used for materializing the nodes.
 */
TAutoPtr<ITreeBuilder> CreateBuilderFromFactory(INodeFactory* factory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

