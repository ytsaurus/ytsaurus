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

//! Translates YSON events into a series of calls to TJsonWriter
//! thus enabling to transform YSON into JSON.
/*!
 *  \note
 *  Attributes and entities are not supported.
 *  
 *  Explicit #Flush calls should be made when finished writing via the adapter.
 */
// TODO: UTF8 strings
class TJsonAdapter
    : public IYsonConsumer
{
public:
    TJsonAdapter(TOutputStream* output);

    void Flush();

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
    THolder<NJson::TJsonWriter> JsonWriter;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
