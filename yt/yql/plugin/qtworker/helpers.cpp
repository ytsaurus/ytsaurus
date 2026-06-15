#include "helpers.h"

#include <yt/yql/plugin/lib/error_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

bool IsTaskTerminal(NYql::NProto::ETaskStatus status)
{
    return status == NYql::NProto::ETaskStatus::COMPLETED
        || status == NYql::NProto::ETaskStatus::ERROR
        || status == NYql::NProto::ETaskStatus::ABORTED;
}

NYql::NProto::TTaskFile::EType FileTypeToProto(EQueryFileContentType type)
{
    switch (type) {
        case EQueryFileContentType::RawInlineData:
            return NYql::NProto::TTaskFile::CONTENT;
        case EQueryFileContentType::Url:
            return NYql::NProto::TTaskFile::URL;
        default:
            THROW_ERROR_EXCEPTION("Unexpected file content")
                << TErrorAttribute("type", static_cast<int>(type));
    }
}

NYql::NProto::ETaskAction ExecuteModeToProto(int executeMode)
{
    switch (executeMode) {
        case 0:
            return NYql::NProto::ETaskAction::VALIDATE;
        case 1:
            return NYql::NProto::ETaskAction::OPTIMIZE;
        case 2:
            return NYql::NProto::ETaskAction::RUN;
        default:
            THROW_ERROR_EXCEPTION("Unknown execute mode %Qv", executeMode);
    }
}

void UpdateTaskResultData(NYql::NProto::TTaskResult& to, const NYql::NProto::TTaskResult& from)
{
    if (from.HasAst()) {
        to.SetAst(from.GetAst());
    }
    if (from.HasPlan()) {
        to.SetPlan(from.GetPlan());
    }
    if (from.HasStatus()) {
        to.SetStatus(from.GetStatus());
    }
    if (from.HasStatistics()) {
        to.SetStatistics(from.GetStatistics());
    }
    if (from.IssuesSize() > 0) {
        *to.MutableIssues() = from.GetIssues();
    }
    if (from.ErrorsSize() > 0) {
        *to.MutableErrors() = from.GetErrors();
    }
    if (from.ResultsSize() > 0) {
        *to.MutableResults() = from.GetResults();
    }
    if (from.HasRevision() && from.GetRevision() != 0) {
        to.SetRevision(from.GetRevision());
    }
}

TString BuildYsonResultList(const NYql::NProto::TTaskResult& result)
{
    if (result.ResultsSize() == 0) {
        return {};
    }
    return NYTree::BuildYsonStringFluently()
        .DoListFor(result.GetResults(), [] (NYTree::TFluentList fluent, const TString& item) {
            fluent.Item().Value(NYson::TYsonStringBuf(item));
        })
        .ToString();
}

TQueryResult TaskResultToYqlResult(const NYql::NProto::TTaskResult& result, TString progress)
{
    if (result.IssuesSize() > 0) {
        NYql::TIssues issues;
        IssuesFromMessage(result.GetIssues(), issues);
        return TQueryResult{
            .YsonError = IssuesToYtErrorYson(issues),
        };
    }
    if (result.ErrorsSize() > 0) {
        return TQueryResult{
            .YsonError = MessageToYtErrorYson(result.GetErrors(0).GetMessage()),
        };
    }
    if (result.GetStatus() == NYql::NProto::ETaskStatus::ERROR) {
        return TQueryResult{
            .YsonError = MessageToYtErrorYson("Query finished with ERROR status on worker"),
        };
    }

    TString ysonResult = BuildYsonResultList(result);
    return TQueryResult{
        .YsonResult = ysonResult ? std::make_optional(ysonResult) : std::nullopt,
        .Plan = result.HasPlan() ? std::make_optional(result.GetPlan()) : std::nullopt,
        .Statistics = result.HasStatistics() ? std::make_optional(result.GetStatistics()) : std::nullopt,
        .Progress = std::move(progress),
        .Ast = result.HasAst() ? std::make_optional(result.GetAst()) : std::nullopt,
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
