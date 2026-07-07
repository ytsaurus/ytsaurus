#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/job_report.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

//! A job report is built fluently and consumed once. Each setter mutates |*this| and
//! returns |TNodeJobReport&&|, so a report can be assembled on a temporary and handed
//! straight to a |TJobReport&&| sink:
//!     HandleJobReport(TNodeJobReport().Type(...).State(...));
//! and an already-built report can be patched in place, with the result discarded:
//!     auto report = TNodeJobReport().Type(...).State(...);
//!     if (FinishTime_) {
//!         report.FinishTime(*FinishTime_);
//!     }
//!     return report;
//!
//! HAZARD: because the setters return an rvalue reference to |*this|, binding a
//! setter's result to a value moves out of the report. After
//!     auto copy = report.FinishTime(...);
//! |report| is left moved-from. When patching a named report, discard the setter's
//! result (as above); capture it only when handing the report off -- a fresh chain,
//! or |std::move(report).Foo(...)| into a sink.
class TNodeJobReport
    : public NServer::TJobReport
{
public:
    TNodeJobReport&& OperationId(TOperationId operationId);
    TNodeJobReport&& JobId(TJobId jobId);
    TNodeJobReport&& Type(EJobType type);
    TNodeJobReport&& State(EJobState state);
    TNodeJobReport&& StartTime(TInstant startTime);
    TNodeJobReport&& FinishTime(TInstant finishTime);
    TNodeJobReport&& Error(const TError& error);
    TNodeJobReport&& InterruptionInfo(NServer::TJobInterruptionInfo interruptionInfo);
    TNodeJobReport&& Spec(const NControllerAgent::NProto::TJobSpec& spec);
    TNodeJobReport&& SpecVersion(i64 specVersion);
    TNodeJobReport&& Statistics(const NYson::TYsonString& statistics);
    TNodeJobReport&& Events(const NServer::TJobEvents& events);
    TNodeJobReport&& StderrSize(i64 stderrSize);
    TNodeJobReport&& Stderr(const std::string& stderr);
    TNodeJobReport&& GpuCheckStderr(std::string gpuCheckStderr);
    TNodeJobReport&& FailContext(const std::string& failContext);
    TNodeJobReport&& Profile(const NJobAgent::TJobProfile& profile);
    TNodeJobReport&& CoreInfos(NControllerAgent::TCoreInfos coreInfos);
    TNodeJobReport&& ExecAttributes(const NYson::TYsonString& execAttributes);
    TNodeJobReport&& TreeId(std::string treeId);
    TNodeJobReport&& MonitoringDescriptor(std::string monitoringDescriptor);
    TNodeJobReport&& Address(std::optional<std::string> address);
    TNodeJobReport&& Addresses(std::optional<NNodeTrackerClient::TAddressMap> addresses);
    TNodeJobReport&& ArchiveFeatures(const NYson::TYsonString& archiveFeatures);
    TNodeJobReport&& JobCompetitionId(TJobId jobCompetitionId);
    TNodeJobReport&& ProbingJobCompetitionId(TJobId probingJobCompetitionId);
    TNodeJobReport&& TaskName(std::string taskName);
    TNodeJobReport&& Ttl(TDuration ttl);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
