#pragma once

#include "public.h"

#include <yt/yt/server/lib/controller_agent/public.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/ytlib/job_tracker_client/public.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/yson_struct.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/misc/property.h>

namespace NYT::NJobAgent {

////////////////////////////////////////////////////////////////////////////////

struct TTimeStatistics
{
    std::optional<TDuration> PrepareDuration;
    std::optional<TDuration> ArtifactsDownloadDuration;
    std::optional<TDuration> PrepareRootFSDuration;
    std::optional<TDuration> ExecDuration;
    std::optional<TDuration> GpuCheckDuration;

    void Persist(const TStreamPersistenceContext& context);

    void AddSamplesTo(TStatistics* statistics) const;

    bool IsEmpty() const;
};

void ToProto(
    NControllerAgent::NProto::TTimeStatistics* timeStatisticsProto,
    const TTimeStatistics& timeStatistics);
void FromProto(
    TTimeStatistics* timeStatistics,
    const NControllerAgent::NProto::TTimeStatistics& timeStatisticsProto);

void Serialize(const TTimeStatistics& timeStatistics, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobEvent
{
    explicit TJobEvent(NJobTrackerClient::EJobState state);
    explicit TJobEvent(NJobTrackerClient::EJobPhase phase);
    TJobEvent(NJobTrackerClient::EJobState state, NJobTrackerClient::EJobPhase phase);

    DEFINE_BYREF_RO_PROPERTY(TInstant, Timestamp)
    DEFINE_BYREF_RO_PROPERTY(std::optional<NJobTrackerClient::EJobState>, State)
    DEFINE_BYREF_RO_PROPERTY(std::optional<NJobTrackerClient::EJobPhase>, Phase)
};

using TJobEvents = std::vector<TJobEvent>;

void Serialize(const TJobEvents& events, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobProfile
{
    TString Type;
    TString Blob;
    double ProfilingProbability;
};

////////////////////////////////////////////////////////////////////////////////

struct TJobReport
{
    size_t EstimateSize() const;

    TJobReport ExtractSpec() const;
    TJobReport ExtractStderr() const;
    TJobReport ExtractFailContext() const;
    TJobReport ExtractProfile() const;
    TJobReport ExtractIds() const;

    bool IsEmpty() const;

    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TOperationId, OperationId)
    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TJobId, JobId)
    DEFINE_BYREF_RO_PROPERTY(std::optional<NJobTrackerClient::EJobType>, Type)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, State)
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, StartTime)
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, FinishTime)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Error)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Spec)
    DEFINE_BYREF_RO_PROPERTY(std::optional<i64>, SpecVersion)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Statistics)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Events)
    DEFINE_BYREF_RO_PROPERTY(std::optional<ui64>, StderrSize)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, Stderr)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, FailContext)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TJobProfile>, Profile)
    DEFINE_BYREF_RO_PROPERTY(std::optional<NScheduler::TCoreInfos>, CoreInfos)
    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TJobId, JobCompetitionId)
    DEFINE_BYREF_RO_PROPERTY(NJobTrackerClient::TJobId, ProbingJobCompetitionId)
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, HasCompetitors)
    DEFINE_BYREF_RO_PROPERTY(std::optional<bool>, HasProbingCompetitors)
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, ExecAttributes);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, TaskName);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, TreeId);
    DEFINE_BYREF_RO_PROPERTY(std::optional<TString>, MonitoringDescriptor);
    DEFINE_BYREF_RO_PROPERTY(std::optional<ui64>, JobCookie);

protected:
    TJobReport() = default;
};

struct TControllerJobReport
    : public TJobReport
{
    TControllerJobReport OperationId(NJobTrackerClient::TOperationId operationId);
    TControllerJobReport JobId(NJobTrackerClient::TJobId jobId);
    TControllerJobReport HasCompetitors(bool hasCompetitors, NControllerAgent::EJobCompetitionType);
    TControllerJobReport JobCookie(ui64 jobCookie);
};

struct TNodeJobReport
    : public TJobReport
{
    TNodeJobReport OperationId(NJobTrackerClient::TOperationId operationId);
    TNodeJobReport JobId(NJobTrackerClient::TJobId jobId);
    TNodeJobReport Type(NJobTrackerClient::EJobType type);
    TNodeJobReport State(NJobTrackerClient::EJobState state);
    TNodeJobReport StartTime(TInstant startTime);
    TNodeJobReport FinishTime(TInstant finishTime);
    TNodeJobReport Error(const TError& error);
    TNodeJobReport Spec(const NControllerAgent::NProto::TJobSpec& spec);
    TNodeJobReport SpecVersion(i64 specVersion);
    TNodeJobReport Statistics(const NYson::TYsonString& statistics);
    TNodeJobReport Events(const TJobEvents& events);
    TNodeJobReport StderrSize(i64 stderrSize);
    TNodeJobReport Stderr(const TString& stderr);
    TNodeJobReport FailContext(const TString& failContext);
    TNodeJobReport Profile(const TJobProfile& profile);
    TNodeJobReport CoreInfos(NScheduler::TCoreInfos coreInfos);
    TNodeJobReport ExecAttributes(const NYson::TYsonString& execAttributes);
    TNodeJobReport TreeId(TString treeId);
    TNodeJobReport MonitoringDescriptor(TString monitoringDescriptor);

    void SetStatistics(const NYson::TYsonString& statistics);
    void SetStartTime(TInstant startTime);
    void SetFinishTime(TInstant finishTime);
    void SetJobCompetitionId(NJobTrackerClient::TJobId jobCompetitionId);
    void SetProbingJobCompetitionId(NJobTrackerClient::TJobId CompetitionId);
    void SetTaskName(const TString& taskName);
};

////////////////////////////////////////////////////////////////////////////////

struct TGpuDevice
    : public NYTree::TYsonStruct
{
    int DeviceNumber;

    TString DeviceName;

    REGISTER_YSON_STRUCT(TGpuDevice);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGpuDevice);

struct TExecAttributes
    : public NYTree::TYsonStructLite
{
    //! Job slot index.
    int SlotIndex = -1;

    //! Job container IP addresses.
    //! If job is not using network isolation its IPs
    //! coincide with node's IPs.
    std::vector<TString> IPAddresses;

    //! Absolute path to job sandbox directory.
    TString SandboxPath;

    //! Medium of disk acquired by slot.
    TString MediumName;

    //! GPU devices used by job.
    std::vector<TIntrusivePtr<TGpuDevice>> GpuDevices;

    REGISTER_YSON_STRUCT(TExecAttributes);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
