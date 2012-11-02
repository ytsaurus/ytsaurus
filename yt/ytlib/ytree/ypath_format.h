#pragma once

// TODO(babenko): remove this file once table selectors are moved to rich YPath attributes

#include "public.h"
#include <ytlib/yson/token.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//const ETokenType::EDomain PathSeparatorToken = ETokenType::Slash;
//const ETokenType::EDomain GoToAttributesToken = ETokenType::At;
//const ETokenType::EDomain RootToken = ETokenType::Slash;
//const ETokenType::EDomain NodeGuidMarkerToken = ETokenType::Hash;
//const ETokenType::EDomain TransactionMarkerToken = ETokenType::Bang;
//const ETokenType::EDomain ListAppendToken = ETokenType::Plus;
//const ETokenType::EDomain ListInsertToken = ETokenType::Caret;
//const ETokenType::EDomain HomeDirToken = ETokenType::Tilde;
//const ETokenType::EDomain SuppressRedirectToken = ETokenType::Ampersand;
//const ETokenType::EDomain WildcardToken = ETokenType::Asterisk;

// Table selectors
const NYson::ETokenType::EDomain BeginColumnSelectorToken = NYson::ETokenType::LeftBrace;
const NYson::ETokenType::EDomain EndColumnSelectorToken = NYson::ETokenType::RightBrace;
const NYson::ETokenType::EDomain ColumnSeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType::EDomain BeginRowSelectorToken = NYson::ETokenType::LeftBracket;
const NYson::ETokenType::EDomain EndRowSelectorToken = NYson::ETokenType::RightBracket;
const NYson::ETokenType::EDomain RowIndexMarkerToken = NYson::ETokenType::Hash;
const NYson::ETokenType::EDomain BeginTupleToken = NYson::ETokenType::LeftParenthesis;
const NYson::ETokenType::EDomain EndTupleToken = NYson::ETokenType::RightParenthesis;
const NYson::ETokenType::EDomain KeySeparatorToken = NYson::ETokenType::Comma;
const NYson::ETokenType::EDomain RangeToken = NYson::ETokenType::Colon;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
