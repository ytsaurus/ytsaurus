#pragma once

#include <contrib/ydb/library/yql/ast/yql_ast.h>
#include <contrib/ydb/library/yql/parser/lexer_common/hints.h>
#include <contrib/ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <contrib/ydb/library/yql/public/issue/yql_warning.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue_manager.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {
    struct TTranslationSettings;
}

namespace NSQLTranslationV1 {

    NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const NSQLTranslation::TSQLHints& hints, const NSQLTranslation::TTranslationSettings& settings);

}  // namespace NSQLTranslationV1
