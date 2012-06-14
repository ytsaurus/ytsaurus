#pragma once

#include "public.h"
#include "token.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

const ETokenType::EDomain PathSeparatorToken = ETokenType::Slash;
const ETokenType::EDomain GoToAttributesToken = ETokenType::At;
const ETokenType::EDomain RootToken = ETokenType::Slash;
const ETokenType::EDomain NodeGuidMarkerToken = ETokenType::Hash;
const ETokenType::EDomain TransactionMarkerToken = ETokenType::Bang;
const ETokenType::EDomain ListAppendToken = ETokenType::Plus;
const ETokenType::EDomain ListInsertToken = ETokenType::Caret;
const ETokenType::EDomain HomeDirToken = ETokenType::Tilde;
const ETokenType::EDomain SuppressRedirectToken = ETokenType::Ampersand;
const ETokenType::EDomain RemoveAllToken = ETokenType::Asterisk;

// Table selectors
const ETokenType::EDomain BeginColumnSelectorToken = ETokenType::LeftBrace;
const ETokenType::EDomain EndColumnSelectorToken = ETokenType::RightBrace;
const ETokenType::EDomain ColumnSeparatorToken = ETokenType::Comma;
const ETokenType::EDomain BeginRowSelectorToken = ETokenType::LeftBracket;
const ETokenType::EDomain EndRowSelectorToken = ETokenType::RightBracket;
const ETokenType::EDomain RowIndexMarkerToken = ETokenType::Hash;
const ETokenType::EDomain BeginTupleToken = ETokenType::LeftParenthesis;
const ETokenType::EDomain EndTupleToken = ETokenType::RightParenthesis;
const ETokenType::EDomain KeySeparatorToken = ETokenType::Comma;
const ETokenType::EDomain RangeToken = ETokenType::Colon;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
