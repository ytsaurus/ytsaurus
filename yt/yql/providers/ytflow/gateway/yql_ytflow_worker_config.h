#pragma once

#include <library/cpp/yson/node/node.h>

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/yql_user_data.h>

#include <yt/yql/providers/ytflow/gateway/yql_ytflow_pipeline_spec.h>

#include <util/generic/maybe.h>


namespace NYql {

struct TYtflowSettings;

} // namespace NYql

namespace NYql::NYtflow {

class TConfigClusters;

} // namespace NYql::NYtflow

namespace NYql::NYtflow {

inline constexpr TStringBuf WORKER_LOGS_TABLE = "worker_logs";
inline constexpr TStringBuf CONTROLLER_LOGS_TABLE = "controller_logs";

namespace NPrivate {

TMaybe<NYT::TNode> SerializeUseCpuAwareBalancer(
    TMaybe<bool> useCpuAwareBalancer);

} // namespace NPrivate

NYT::TNode MakeWorkerConfig(
    const TYqlOperationOptions& operationOptions,
    const TYtflowSettings& config,
    const TConfigClusters& configClusters,
    const TUserDataTable& userDataBlocks,
    const TVector<TFile>& files);

} // namespace NYql::NYtflow
