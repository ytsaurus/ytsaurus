#pragma once

#include "token.h"

namespace NYT {
namespace NYson {

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

//! Separates items in lists.
const ETokenType::EDomain ListItemSeparatorToken = ETokenType::Semicolon;
//! Separates items in maps, attributes.
const ETokenType::EDomain KeyedItemSeparatorToken = ETokenType::Semicolon;
//! Separates keys from values in maps.
const ETokenType::EDomain KeyValueSeparatorToken = ETokenType::Equals;

//! Indicates an entity.
const ETokenType::EDomain EntityToken = ETokenType::Hash;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT

