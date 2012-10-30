#pragma once

#include "public.h"
#include "config.h"

#include <ytlib/ytree/forwarding_yson_consumer.h>
#include <ytlib/ytree/yson_writer.h>

#include <library/json/json_writer.h>

namespace NYT {
namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

// YSON-to-JSON Mapping Conventions
//
// * ListFragments and MapFragments (which exist in YSON) are not supported.
// * Bool type (which exists in JSON) is not supported.
// * Other types (without attributes) are mapped almost as is:
//      YSON <----> JSON
//    * List <---> Array
//    * Map  <---> Object
//    * Int  <---> Int
//    * Double <---> Double
//    * String (s) <---> String (t):
//      * If s[0] != '&' and s is a valid UTF8 string: t := s
//      * else: t := '&' + Base64(s)
//    * Entity <---> null
// * Nodes with attributes are mapped to the following JSON map:
//    {
//        '$attributes': (attributes map),
//        '$value': (value, as explained above)
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
class TJsonWriter
    : public NYTree::TYsonConsumerBase
{
public:
    TJsonWriter(TOutputStream* output, TJsonFormatConfigPtr config = NULL);
    ~TJsonWriter();

    void Flush();

    virtual void OnStringScalar(const TStringBuf& value);
    virtual void OnIntegerScalar(i64 value);
    virtual void OnDoubleScalar(double value);

    virtual void OnEntity();

    virtual void OnBeginList();
    virtual void OnListItem();
    virtual void OnEndList();

    virtual void OnBeginMap();
    virtual void OnKeyedItem(const TStringBuf& key);
    virtual void OnEndMap();

    virtual void OnBeginAttributes();
    virtual void OnEndAttributes();

private:
    TJsonWriter(NJson::TJsonWriter* jsonWriter, TJsonFormatConfigPtr config);

    THolder<NJson::TJsonWriter> UnderlyingJsonWriter;
    NJson::TJsonWriter* JsonWriter;
    TJsonFormatConfigPtr Config;

    void WriteStringScalar(const TStringBuf& value);

    void EnterNode();
    void LeaveNode();
    bool IsWriteAllowed();

    std::vector<bool> HasUnfoldedStructureStack_;
    int InAttributesBalance_;
    bool HasAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
