#pragma once

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

//! A job report is built fluently and consumed once. Each setter mutates |*this| and
//! returns |TControllerJobReport&&|, so a report can be assembled on a temporary and
//! handed straight to a |TJobReport&&| sink:
//!     HandleJobReport(joblet, TControllerJobReport().OperationId(...).JobId(...));
//! and an already-built report can be patched in place, with the result discarded.
//!
//! HAZARD: because the setters return an rvalue reference to |*this|, binding a
//! setter's result to a value moves out of the report. After
//!     auto copy = report.FinishTime(...);
//! |report| is left moved-from. When patching a named report, discard the setter's
//! result; capture it only when handing the report off -- a fresh chain, or
//! |std::move(report).Foo(...)| into a sink.
class TControllerJobReport
    : public NServer::TJobReport
{
public:
    TControllerJobReport&& OperationId(TOperationId operationId);
    TControllerJobReport&& JobId(TJobId jobId);
    TControllerJobReport&& HasCompetitors(bool hasCompetitors, EJobCompetitionType competitionType);
    TControllerJobReport&& JobCookie(ui64 jobCookie);
    TControllerJobReport&& CollectiveMemberRank(ui64 index);
    TControllerJobReport&& CollectiveId(TGuid collectiveId);
    TControllerJobReport&& Address(std::optional<std::string> address);
    TControllerJobReport&& Addresses(std::optional<NNodeTrackerClient::TAddressMap> addresses);
    TControllerJobReport&& ControllerState(EJobState controllerState);
    TControllerJobReport&& Ttl(std::optional<TDuration> ttl);
    TControllerJobReport&& OperationIncarnation(std::string operationIncarnation);
    TControllerJobReport&& AllocationId(TAllocationId allocationId);
    TControllerJobReport&& StartTime(TInstant startTime);
    TControllerJobReport&& FinishTime(TInstant finishTime);
    TControllerJobReport&& GangRank(std::optional<i64> gangRank);
    TControllerJobReport&& Error(const TError& error);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
