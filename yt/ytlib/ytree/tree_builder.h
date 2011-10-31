#pragma once

#include "common.h"
#include "ytree.h"
#include "yson_events.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Reconstructs a YTree from IYsonConsumer calls.
class TTreeBuilder
    : public IYsonConsumer
{
public:
    //! Initializes an instance.
    /*!
     *  \param factory A factory used for materializing the nodes.
     */
    TTreeBuilder(INodeFactory* factory);

    //! Returns the root node of the constructed tree.
    /*!
     *  \note
     *  Must be called after the tree is constructed.
     */
    INode::TPtr GetRoot() const;

    //! Creates a YSON builder.
    static TYsonBuilder::TPtr CreateYsonBuilder(INodeFactory* factory);

    virtual void OnStringScalar(const Stroka& value, bool hasAttributes);
    virtual void OnInt64Scalar(i64 value, bool hasAttributes);
    virtual void OnDoubleScalar(double value, bool hasAttributes);
    virtual void OnEntity(bool hasAttributes);

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList(bool hasAttributes);

    virtual void OnBeginMap();
    virtual void OnMapItem(const Stroka& name);
    virtual void OnEndMap(bool hasAttributes);

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const Stroka& name);
    virtual void OnEndAttributes();

private:
    INodeFactory* Factory;
    yvector<INode::TPtr> Stack;

    static INode::TPtr YsonBuilderThunk(
        TYsonProducer::TPtr producer,
        INodeFactory* factory);

    void AddToList();
    void AddToMap();

    void Push(INode::TPtr node);
    INode::TPtr Pop();
    INode::TPtr Peek();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

