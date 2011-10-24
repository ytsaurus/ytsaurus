#pragma once

#include "common.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

const char BeginListSymbol = '[';
const char EndListSymbol = ']';

const char BeginMapSymbol = '{';
const char EndMapSymbol = '}';

const char BeginAttributesSymbol = '<';
const char EndAttributesSymbol = '>';

const char ListItemSeparator = ';';
const char MapItemSeparator = ';';
const char KeyValueSeparator = '=';

// Indicates start of binary data of specific type
const char Int64Marker = char(1);
const char DoubleMarker = char(2);
const char StringMarker = char(3);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

