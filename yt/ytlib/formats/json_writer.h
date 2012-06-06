#pragma once

#include "public.h"

#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>

#include <library/json/json_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// How yson is mapped into json?
// * Simple types (without attributes) are mapped almost as is:
//     YSon <----> Json
//    * List <---> Array
//    * Map  <---> Object
//    * Int  <---> Int
//    * Double <---> Double
//    * String (s) <---> String (t):
//      * If s[0] != '&' and s is valid UTF8 string: t := s
//      * else: t := '&' + Base64(s)
//    * Not supported (yexception) <---> null
//    * Not supported (yexception) <---> bool
//    * Entity?
// * Types with attributes are mapped to Json object:
//    {
//        '$attributes': attributes->AsMap(),
//        '$value': value (see conversion of basic types)
//    }

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
class TJsonWriter
    : public NYTree::TForwardingYsonConsumer
{
public:
    TJsonWriter(TOutputStream* output, TJsonFormatConfigPtr config = NULL);

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
    THolder<NJson::TJsonWriter> JsonWriter;
    NYTree::TYson Attributes;
    TStringOutput AttributesOutput;
    NYTree::TYsonWriter AttributesWriter;
    TJsonFormatConfigPtr Config;

    void FlushAttributes();
    void DiscardAttributes();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
