#pragma once

#include <contrib/ydb/library/yql/ast/yql_ast.h>
#include <contrib/ydb/library/yql/parser/proto_ast/proto_ast.h>
#include <contrib/ydb/library/yql/public/issue/yql_warning.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue_manager.h>
#include <contrib/ydb/library/yql/sql/settings/translation_settings.h>

#include <google/protobuf/message.h>

namespace NSQLTranslationV0 {

    NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules = nullptr);
    google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NYql::TIssues& err, size_t maxErrors, google::protobuf::Arena* arena = nullptr);
    NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst, const NSQLTranslation::TTranslationSettings& settings);

}  // namespace NSQLTranslationV0
