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

    virtual void BeginTree() = 0;
    virtual void EndTree() = 0;

    virtual void StringScalar(const Stroka& value) = 0;
    virtual void Int64Scalar(i64 value) = 0;
    virtual void DoubleScalar(double value) = 0;
    virtual void EntityScalar() = 0;

    virtual void BeginList() = 0;
    virtual void ListItem(int index) = 0;
    virtual void EndList() = 0;

    virtual void BeginMap() = 0;
    virtual void MapItem(const Stroka& name) = 0;
    virtual void EndMap() = 0;

    virtual void BeginAttributes() = 0;
    virtual void AttributesItem(const Stroka& name) = 0;
    virtual void EndAttributes() = 0;
};

typedef IParamAction<IYsonConsumer*> TYsonProducer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

