#pragma once

#include "common.h"
#include "yson_events.h"

#include <dict/json/json.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TJsonAdapter
    : public IYsonConsumer
{
public:
    TJsonAdapter(TJsonWriter* jsonWriter);

    virtual void BeginTree();
    virtual void EndTree();

    virtual void StringValue(const Stroka& value);
    virtual void Int64Value(i64 value);
    virtual void DoubleValue(double value);
    virtual void EntityValue();

    virtual void BeginList();
    virtual void ListItem(int index);
    virtual void EndList();

    virtual void BeginMap();
    virtual void MapItem(const Stroka& name);
    virtual void EndMap();

    virtual void BeginAttributes();
    virtual void AttributesItem(const Stroka& name);
    virtual void EndAttributes();

private:
    TJsonWriter* JsonWriter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
