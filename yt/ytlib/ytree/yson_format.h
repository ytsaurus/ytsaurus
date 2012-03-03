#pragma once

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Indicates the beginning of a list.
const char BeginListSymbol = '[';
//! Indicates the end of a list.
const char EndListSymbol = ']';

//! Indicates the beginning of a map.
const char BeginMapSymbol = '{';
//! Indicates the end of a map.
const char EndMapSymbol = '}';

//! Indicates the beginning of an attribute map.
const char BeginAttributesSymbol = '<';
//! Indicates the end of an attribute map.
const char EndAttributesSymbol = '>';

//! Separates items in list, map, attributes.
const char ItemSeparator = ';';
//! Separates keys from values in maps.
const char KeyValueSeparator = '=';

//! Marks the beginning of a binary i64 literal.
const char Int64Marker = char(1);
//! Marks the beginning of a binary double literal.
const char DoubleMarker = char(2);
//! Marks the beginning of a binary string literal.
const char StringMarker = char(3);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

