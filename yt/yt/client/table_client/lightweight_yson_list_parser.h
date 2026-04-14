#pragma once

#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/yson/lexer.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/token.h>

#include <library/cpp/yt/coding/varint.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <class OnValue>
void DoUnpackValuesTyped(
    OnValue onValue,
    TStringBuf data,
    EValueType type,
    NYson::TStatelessLexer* lexer = nullptr,
    NYson::TToken* token = nullptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient

#define LIGHTWEIGHT_YSON_LIST_PARSER_INL_H_
#include "lightweight_yson_list_parser-inl.h"
#undef LIGHTWEIGHT_YSON_LIST_PARSER_INL_H_
