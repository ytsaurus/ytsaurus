#pragma once

#include "forwarding_yson_consumer.h"
#include "yson_writer.h"

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
    virtual void OnMyKeyedItem(const TStringBuf& key);
    virtual void OnMyEndMap();

    virtual void OnMyBeginAttributes();
    virtual void OnMyEndAttributes();

private:
    void WriteAttributes();

    THolder<NJson::TJsonWriter> JsonWriter;
    TYson Attributes;
    TStringOutput AttributesOutput;
    TYsonWriter AttributesWriter;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
