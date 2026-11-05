#include "describe_workers.h"
#include "intermediate_description.h"

#include <yt/yt/flow/library/cpp/common/flow_view.h>

namespace NYT::NFlow::NDescribe {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void TWorkersDescription::Register(TRegistrar registrar)
{
    registrar.Parameter("workers", &TThis::Workers)
        .Default();
}

////////////////////////////////////////////////////////////////////////////////

TWorkersDescription DescribeWorkers(const TFlowViewPtr& flowView)
{
    TWorkersDescription description;

    auto workerPartitions = GetWorkersPartitionIntermediateDescriptions(flowView);

    for (const auto& [workerAddress, worker] : flowView->State->Workers) {
        TWorkerDescription workerDescription;
        std::vector<TMessage> messages; // Filled messages are ignored.

        FillWorkerDescription(flowView, worker, GetOrDefault(workerPartitions, workerAddress), workerDescription, messages);

        workerDescription.Partitions.clear();
        description.Workers.push_back(std::move(workerDescription));
    }

    return description;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NDescribe
