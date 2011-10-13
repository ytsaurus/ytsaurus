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

    virtual void OnStringScalar(const Stroka& value);
    virtual void OnInt64Scalar(i64 value);
    virtual void OnDoubleScalar(double value);
    virtual void OnEntityScalar();

    virtual void OnBeginList();
    virtual void OnListItem(int index);
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnMapItem(const Stroka& name);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnAttributesItem(const Stroka& name);
    virtual void OnEndAttributes();

private:
    THolder<NJson::TJsonWriter> JsonWriter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
