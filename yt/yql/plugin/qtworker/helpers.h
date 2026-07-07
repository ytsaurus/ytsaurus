#pragma once

#include <yt/yql/plugin/plugin.h>

#include <yql/tools/yqlworker/interface/proto/task.pb.h>

namespace NYT::NYqlPlugin {

////////////////////////////////////////////////////////////////////////////////

bool IsTaskTerminal(NYql::NProto::ETaskStatus status);
NYql::NProto::TTaskFile::EType FileTypeToProto(EQueryFileContentType type);
NYql::NProto::ETaskAction ExecuteModeToProto(int executeMode);

// Accumulates an incremental task result delta: only the fields present
// in the delta are updated, so previously received fields are preserved.
void UpdateTaskResultData(NYql::NProto::TTaskResult& to, const NYql::NProto::TTaskResult& from);

TQueryResult TaskResultToYqlResult(const NYql::NProto::TTaskResult& result, TString progressYson);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
