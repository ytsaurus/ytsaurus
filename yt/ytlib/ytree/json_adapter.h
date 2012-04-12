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
 *  Entities are translated to empty maps.
 *  
 *  Attributes are only supported for entities and maps.
 *  They are written as an inner "$attributes" map.
 *  
 *  Explicit #Flush calls should be made when finished writing via the adapter.
 */
// XXX(babenko): YSON strings vs JSON strings.
class TJsonAdapter
    : public TForwardingYsonConsumer
{
public:
    TJsonAdapter(TOutputStream* output);

    void Flush();

    virtual void OnMyStringScalar(const TStringBuf& value);
    virtual void OnMyIntegerScalar(i64 value);
    virtual void OnMyDoubleScalar(double value);

    virtual void OnMyEntity();

    virtual void OnMyBeginList();
    virtual void OnMyListItem();
    virtual void OnMyEndList();

    virtual void OnMyBeginMap();
    virtual void OnMyMapItem(const TStringBuf& name);
    virtual void OnMyEndMap();

    virtual void OnMyBeginAttributes();
    //virtual void OnMyAttributesItem(const TStringBuf& name);
    //virtual void OnMyEndAttributes();

private:
    THolder<NJson::TJsonWriter> JsonWriter;
    bool WriteAttributes;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
