#pragma once

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TControllerJobReport
    : public NServer::TJobReport
{
public:
    TControllerJobReport OperationId(TOperationId operationId);
    TControllerJobReport JobId(TJobId jobId);
    TControllerJobReport HasCompetitors(bool hasCompetitors, EJobCompetitionType competitionType);
    TControllerJobReport JobCookie(ui64 jobCookie);
    TControllerJobReport Address(std::optional<std::string> address);
    TControllerJobReport Addresses(std::optional<NNodeTrackerClient::TAddressMap> addresses);
    TControllerJobReport ControllerState(EJobState controllerState);
    TControllerJobReport Ttl(std::optional<TDuration> ttl);
    TControllerJobReport OperationIncarnation(std::string operationIncarnation);
    TControllerJobReport AllocationId(TAllocationId allocationId);
    TControllerJobReport StartTime(TInstant startTime);
    TControllerJobReport FinishTime(TInstant finishTime);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
