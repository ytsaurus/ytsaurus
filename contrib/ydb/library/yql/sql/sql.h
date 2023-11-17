#pragma once

#include <contrib/ydb/library/yql/parser/lexer_common/hints.h>
#include <contrib/ydb/library/yql/parser/lexer_common/lexer.h>
#include <contrib/ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <contrib/ydb/library/yql/public/issue/yql_warning.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue_manager.h>
#include <contrib/ydb/library/yql/ast/yql_ast.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

#include <google/protobuf/message.h>

namespace NSQLTranslation {

    NYql::TAstParseResult SqlToYql(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules = nullptr, ui16* actualSyntaxVersion = nullptr);
    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& issues, size_t maxErrors,
        const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    ILexer::TPtr SqlLexer(const TString& query, NYql::TIssues& issues, const TTranslationSettings& settings = {}, ui16* actualSyntaxVersion = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const TSQLHints& hints, const TTranslationSettings& settings);

}  // namespace NSQLTranslationV0
