#include "helpers.h"

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NControllerAgent {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

bool IsFinishedState(EControllerState state)
{
    return state == NControllerAgent::EControllerState::Completed ||
        state == NControllerAgent::EControllerState::Failed ||
        state == NControllerAgent::EControllerState::Aborted;
}

TYsonString BuildBriefStatistics(const INodePtr& statistics)
{
    if (statistics->GetType() != ENodeType::Map) {
        return BuildYsonStringFluently()
            .BeginMap()
            .EndMap();
    }

    // See NControllerAgent::BuildBriefStatistics(std::unique_ptr<TJobSummary> jobSummary).
    auto rowCount = FindNodeByYPath(statistics, "/data/input/row_count/sum");
    auto uncompressedDataSize = FindNodeByYPath(statistics, "/data/input/uncompressed_data_size/sum");
    auto compressedDataSize = FindNodeByYPath(statistics, "/data/input/compressed_data_size/sum");
    auto dataWeight = FindNodeByYPath(statistics, "/data/input/data_weight/sum");
    auto inputPipeIdleTime = FindNodeByYPath(statistics, "/user_job/pipes/input/idle_time/sum");
    auto jobProxyCpuUsage = FindNodeByYPath(statistics, "/job_proxy/cpu/user/sum");

    return BuildYsonStringFluently()
        .BeginMap()
            .DoIf(static_cast<bool>(rowCount), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_row_count").Value(rowCount->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(uncompressedDataSize), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_uncompressed_data_size").Value(uncompressedDataSize->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(compressedDataSize), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_compressed_data_size").Value(compressedDataSize->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(dataWeight), [&] (TFluentMap fluent) {
                fluent.Item("processed_input_data_weight").Value(dataWeight->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(inputPipeIdleTime), [&] (TFluentMap fluent) {
                fluent.Item("input_pipe_idle_time").Value(inputPipeIdleTime->AsInt64()->GetValue());
            })
            .DoIf(static_cast<bool>(jobProxyCpuUsage), [&] (TFluentMap fluent) {
                fluent.Item("job_proxy_cpu_usage").Value(jobProxyCpuUsage->AsInt64()->GetValue());
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
