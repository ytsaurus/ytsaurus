#pragma once

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TControllerJobReport
    : public TJobReport
{
public:
    TControllerJobReport OperationId(TOperationId operationId);
    TControllerJobReport JobId(TJobId jobId);
    TControllerJobReport HasCompetitors(bool hasCompetitors, EJobCompetitionType competitionType);
    TControllerJobReport JobCookie(ui64 jobCookie);
    TControllerJobReport Address(std::optional<TString> address);
};

////////////////////////////////////////////////////////////////////////////////

}; // namespace NYT::NControllerAgent
