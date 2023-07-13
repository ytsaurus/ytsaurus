#include "job_report.h"

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

TControllerJobReport TControllerJobReport::OperationId(TOperationId operationId)
{
    OperationId_ = operationId;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::JobId(TJobId jobId)
{
    JobId_ = jobId;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::HasCompetitors(bool hasCompetitors, EJobCompetitionType competitionType)
{
    switch (competitionType) {
        case EJobCompetitionType::Speculative:
            HasCompetitors_ = hasCompetitors;
            break;
        case EJobCompetitionType::Probing:
            HasProbingCompetitors_ = hasCompetitors;
            break;
        case EJobCompetitionType::Experiment:
            break;
        default:
            YT_ABORT();
    }
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::JobCookie(ui64 jobCookie)
{
    JobCookie_ = jobCookie;
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::Address(std::optional<TString> address)
{
    Address_ = std::move(address);
    return std::move(*this);
}

TControllerJobReport TControllerJobReport::ControllerState(EJobState controllerState)
{
    ControllerState_ = FormatEnum(controllerState);
    return std::move(*this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
