#pragma once

#include <yt/yql/plugin/plugin.h>

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

bool IsTaskTerminal(NYql::NProto::ETaskStatus status);
NYql::NProto::TTaskFile::EType FileTypeToProto(EQueryFileContentType type);
NYql::NProto::ETaskAction ExecuteModeToProto(int executeMode);
std::optional<TString> ExtractDefaultCluster(const NYql::TGatewaysConfig& config);

// Accumulates an incremental task result delta: only the parts present
// in the delta are updated, so previously received parts are preserved.
void UpdateTaskResultData(
    NYql::NProto::TTaskResult& to,
    const NYql::NProto::TTaskResult& from,
    THashMap<NYqlClient::EProgressPart, ui32>* latestPartRevisions);

TQueryResult TaskResultToYqlResult(
    const NYql::NProto::TTaskResult& result,
    TString progressYson,
    const THashMap<NYqlClient::EProgressPart, ui32>& latestPartRevisions = {},
    std::optional<ui32> revision = std::nullopt);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
