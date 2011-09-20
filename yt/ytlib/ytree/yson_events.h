#pragma once

#include "common.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

struct IYsonEvents
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IYsonEvents> TPtr;

    virtual void BeginTree() = 0;
    virtual void EndTree() = 0;

    virtual void StringValue(const Stroka& value) = 0;
    virtual void Int64Value(i64 value) = 0;
    virtual void DoubleValue(double value) = 0;
    virtual void EntityValue() = 0;

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

