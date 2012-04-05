#pragma once

#include "forwarding_yson_consumer.h"

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
    : public TForwardingYsonConsumer
{
public:
    TJsonAdapter(TOutputStream* output);

    void Flush();

    virtual void OnMyStringScalar(const TStringBuf& value, bool hasAttributes);
    virtual void OnMyIntegerScalar(i64 value, bool hasAttributes);
    virtual void OnMyDoubleScalar(double value, bool hasAttributes);

    virtual void OnMyEntity(bool hasAttributes);

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList(bool hasAttributes);

    virtual void OnMyBeginMap();
    virtual void OnMyMapItem(const TStringBuf& name);
    virtual void OnMyEndMap(bool hasAttributes);

    virtual void OnMyBeginAttributes();
    virtual void OnMyAttributesItem(const TStringBuf& name);
    virtual void OnMyEndAttributes();

private:
    THolder<NJson::TJsonWriter> JsonWriter;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
