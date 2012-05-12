#pragma once

#include "token.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Indicates the beginning of a list.
const ETokenType::EDomain BeginListToken = ETokenType::LeftBracket;
//! Indicates the end of a list.
const ETokenType::EDomain EndListToken = ETokenType::RightBracket;

//! Indicates the beginning of a map.
const ETokenType::EDomain BeginMapToken = ETokenType::LeftBrace;
//! Indicates the end of a map.
const ETokenType::EDomain EndMapToken = ETokenType::RightBrace;

//! Indicates the beginning of an attribute map.
const ETokenType::EDomain BeginAttributesToken = ETokenType::LeftAngle;
//! Indicates the end of an attribute map.
const ETokenType::EDomain EndAttributesToken = ETokenType::RightAngle;

//! Separates items in list, map, attributes.
const ETokenType::EDomain ItemSeparatorToken = ETokenType::Semicolon;
//! Separates keys from values in maps.
const ETokenType::EDomain KeyValueSeparatorToken = ETokenType::Equals;

//! Indicates an entity.
const ETokenType::EDomain EntityToken = ETokenType::Hash;

// TODO(roizner): move somewhere
//! Marks the beginning of a binary string literal.
const char StringMarker = char(1);
//! Marks the beginning of a binary i64 literal.
const char IntegerMarker = char(2);
//! Marks the beginning of a binary double literal.
const char DoubleMarker = char(3);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

