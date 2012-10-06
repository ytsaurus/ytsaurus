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
// * List fragments and map fragments (which exist in YSON) are not supported.
// * Boolean type (which exists in JSON) is not supported.
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
 *  Entities are translated to nulls.
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
    explicit TJsonWriter(TOutputStream* output, TJsonFormatConfigPtr config = NULL);
    ~TJsonWriter();

    void Flush();

    virtual void OnStringScalar(const TStringBuf& value) override;
    virtual void OnIntegerScalar(i64 value) override;
    virtual void OnDoubleScalar(double value) override;

    virtual void OnEntity() override;

    virtual void OnBeginList() override;
    virtual void OnListItem() override;
    virtual void OnEndList() override;

    virtual void OnBeginMap() override;
    virtual void OnKeyedItem(const TStringBuf& key) override;
    virtual void OnEndMap() override;

    virtual void OnBeginAttributes() override;
    virtual void OnEndAttributes() override;

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
