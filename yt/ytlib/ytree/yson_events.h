#pragma once

#include "common.h"
#include "ytree_fwd.h"

#include "../actions/action.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

// TODO: document
struct IYsonConsumer
{
    virtual ~IYsonConsumer()
    { }

    virtual void OnStringScalar(const Stroka& value, bool hasAttributes) = 0;
    virtual void OnInt64Scalar(i64 value, bool hasAttributes) = 0;
    virtual void OnDoubleScalar(double value, bool hasAttributes) = 0;
    
    virtual void OnEntity(bool hasAttributes) = 0;

    virtual void OnBeginList() = 0;
    virtual void OnListItem() = 0;
    virtual void OnEndList(bool hasAttributes) = 0;

    virtual void OnBeginMap() = 0;
    virtual void OnMapItem(const Stroka& name) = 0;
    virtual void OnEndMap(bool hasAttributes) = 0;

    virtual void OnBeginAttributes() = 0;
    virtual void OnAttributesItem(const Stroka& name) = 0;
    virtual void OnEndAttributes() = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO: document
typedef IParamAction<IYsonConsumer*> TYsonProducer;
// TODO: document
typedef IParamFunc<TYsonProducer::TPtr, TIntrusivePtr<INode> > TYsonBuilder;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

