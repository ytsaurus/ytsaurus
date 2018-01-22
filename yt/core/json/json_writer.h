#pragma once

#include "public.h"
#include "config.h"

#include <yt/core/yson/public.h>

namespace NYT {
namespace NJson {

////////////////////////////////////////////////////////////////////////////////

// TODO: rewrite this documentation
// YSON-to-JSON Mapping Conventions
//
// * Map fragment (which exists in YSON) is not supported.
// * Boolean type (which exists in JSON) is not supported.
// * List fragments are enclosed in Array.
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

std::unique_ptr<NYson::IFlushableYsonConsumer> CreateJsonConsumer(
    IOutputStream* output,
    NYson::EYsonType type = NYson::EYsonType::Node,
    TJsonFormatConfigPtr config = New<TJsonFormatConfig>());

////////////////////////////////////////////////////////////////////////////////

} // namespace NJson
} // namespace NYT
