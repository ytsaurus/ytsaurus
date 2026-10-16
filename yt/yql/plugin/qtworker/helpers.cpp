#include "helpers.h"

#include <yt/yql/plugin/lib/error_helpers.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <yql/essentials/utils/log/log.h>

namespace NYT::NYqlPlugin {

using namespace NYqlClient;

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
                .With("type", static_cast<int>(type));
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

std::optional<TString> ExtractDefaultCluster(const NYql::TGatewaysConfig& config)
{
    if (config.HasYt()) {
        for (const auto& mapping : config.GetYt().GetClusterMapping()) {
            if (mapping.GetDefault()) {
                return mapping.GetName();
            }
        }
    }
    return {};
}

void UpdateTaskResultData(
    NYql::NProto::TTaskResult& to,
    const NYql::NProto::TTaskResult& from,
    THashMap<EProgressPart, ui32>* latestPartRevisions)
{
    if (from.IssuesSize() > 0) {
        *to.MutableIssues() = from.GetIssues();
    }
    if (from.ResultsSize() > 0) {
        *to.MutableResults() = from.GetResults();
    }
    if (from.HasStatus()) {
        to.SetStatus(from.GetStatus());
    }

    if (from.GetRevision() == 0) {
        YQL_LOG(ERROR) << "Skip revisioned parts of an update with zero revision: " << from.ShortDebugString();
        return;
    }

    if (from.HasAst()) {
        (*latestPartRevisions)[EProgressPart::YqlAst] = from.GetRevision();
        to.SetAst(from.GetAst());
    }
    if (from.HasPlan()) {
        (*latestPartRevisions)[EProgressPart::YqlPlan] = from.GetRevision();
        to.SetPlan(from.GetPlan());
    }
    if (from.HasStatistics()) {
        (*latestPartRevisions)[EProgressPart::YqlStatistics] = from.GetRevision();
        to.SetStatistics(from.GetStatistics());
    }
    to.SetRevision(from.GetRevision());
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

TQueryResult TaskResultToYqlResult(
    const NYql::NProto::TTaskResult& result,
    TString progress,
    const THashMap<EProgressPart, ui32>& latestPartRevisions,
    std::optional<ui32> revision)
{
    auto checkRevision = [&] (EProgressPart part) {
        if (!revision) {
            return true;
        }
        auto it = latestPartRevisions.find(part);
        return it != latestPartRevisions.end() && it->second > *revision;
    };

    TString ysonResult = BuildYsonResultList(result);
    auto queryResult = TQueryResult{
        .YsonResult = ysonResult ? std::make_optional(ysonResult) : std::nullopt,
        .Plan = result.HasPlan() && checkRevision(EProgressPart::YqlPlan) ? std::make_optional(result.GetPlan()) : std::nullopt,
        .Statistics = result.HasStatistics() && checkRevision(EProgressPart::YqlStatistics) ? std::make_optional(result.GetStatistics()) : std::nullopt,
        // Progress carries no revision of its own, so it is always sent.
        .Progress = std::move(progress),
        .Ast = result.HasAst() && checkRevision(EProgressPart::YqlAst) ? std::make_optional(result.GetAst()) : std::nullopt,
        .Revision = result.HasRevision() ? std::make_optional(result.GetRevision()) : std::nullopt,
    };

    if (result.GetStatus() == NYql::NProto::ETaskStatus::ERROR) {
        if (result.IssuesSize() > 0) {
            NYql::TIssues issues;
            IssuesFromMessage(result.GetIssues(), issues);
            queryResult.YsonError = IssuesToYtErrorYson(issues);
        } else {
            queryResult.YsonError = MessageToYtErrorYson("Query finished with ERROR status on worker");
        }
    }

    return queryResult;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
