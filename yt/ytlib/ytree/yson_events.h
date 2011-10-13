#pragma once

#include "common.h"

#include "../actions/action.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IYsonConsumer
{
    virtual ~IYsonConsumer()
    { }

    virtual void OnStringScalar(const Stroka& value) = 0;
    virtual void OnInt64Scalar(i64 value) = 0;
    virtual void OnDoubleScalar(double value) = 0;
    virtual void OnEntityScalar() = 0;

    virtual void OnBeginList() = 0;
    virtual void OnListItem(int index) = 0;
    virtual void OnEndList() = 0;

    virtual void OnBeginMap() = 0;
    virtual void OnMapItem(const Stroka& name) = 0;
    virtual void OnEndMap() = 0;

    virtual void OnBeginAttributes() = 0;
    virtual void OnAttributesItem(const Stroka& name) = 0;
    virtual void OnEndAttributes() = 0;
};

typedef IParamAction<IYsonConsumer*> TYsonProducer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

