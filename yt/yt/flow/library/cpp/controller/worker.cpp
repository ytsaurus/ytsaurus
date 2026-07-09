#include "worker.h"

#include "private.h"

namespace NYT::NFlow::NController {

////////////////////////////////////////////////////////////////////////////////

TWorker::TWorker(
    const TNodeInfoBase& nodeInfo,
    NWorker::TIncarnationId connectionIncarnationId,
    std::vector<TWorkerGroupId> groups,
    THashMap<std::string, ssize_t> capabilities)
{
    static_cast<TNodeInfoBase&>(Info_) = nodeInfo;
    Info_.RegisterTime = TInstant::Now();
    Info_.ConnectionIncarnationId = connectionIncarnationId;
    Info_.Groups = std::move(groups);
    Info_.Capabilities = std::move(capabilities);
}

EWorkerState TWorker::GetState() const
{
    return Info_.State;
}

void TWorker::SetState(EWorkerState state)
{
    Info_.State = state;
}

const TWorkerInfo& TWorker::GetInfo() const
{
    return Info_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
