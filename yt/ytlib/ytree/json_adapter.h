#pragma once

#include "common.h"
#include "yson_events.h"

////////////////////////////////////////////////////////////////////////////////

namespace NJson {
    class TJsonWriter;
}

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TJsonAdapter
    : public IYsonConsumer
{
public:
    TJsonAdapter(TOutputStream* output);
    ~TJsonAdapter();

    virtual void BeginTree();
    virtual void EndTree();

    virtual void StringScalar(const Stroka& value);
    virtual void Int64Scalar(i64 value);
    virtual void DoubleScalar(double value);
    virtual void EntityScalar();

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
    THolder<NJson::TJsonWriter> JsonWriter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
