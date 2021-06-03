#include "job_reporter.h"

#include "config.h"
#include "job_report.h"

#include <yt/yt/server/lib/misc/archive_reporter.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/client/api/connection.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/nonblocking_batch.h>
#include <yt/yt/core/concurrency/async_semaphore.h>

#include <yt/yt/core/utilex/random.h>

namespace NYT::NJobAgent {

using namespace NNodeTrackerClient;
using namespace NTransactionClient;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NControllerAgent;
using namespace NApi;
using namespace NTableClient;
using namespace NTabletClient;
using namespace NProfiling;
using namespace NScheduler;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

namespace {

static const TProfiler ReporterProfiler("/job_reporter");

////////////////////////////////////////////////////////////////////////////////

bool IsSpecEntry(const TJobReport& stat)
{
    return stat.Spec().operator bool();
}

////////////////////////////////////////////////////////////////////////////////

class TJobRowlet
    : public IArchiveRowlet
{
public:
    TJobRowlet(
        TJobReport&& statistics,
        bool reportStatisticsLz4,
        const std::optional<TString>& localAddress)
        : Statistics_(statistics)
        , ReportStatisticsLz4_(reportStatisticsLz4)
        , DefaultLocalAddress_(localAddress)
    { }

    virtual size_t EstimateSize() const override
    {
        return Statistics_.EstimateSize();
    }

    virtual TUnversionedOwningRow ToRow(int archiveVersion) const override
    {
        const auto& index = TJobTableDescriptor::Get().Index;

        TYsonString coreInfosYsonString;
        TString jobCompetitionIdString;
        TString statisticsLz4;
        TYsonString briefStatisticsYsonString;

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[0], index.OperationIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[1], index.OperationIdLo));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[0], index.JobIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[1], index.JobIdLo));
        if (Statistics_.Type()) {
            builder.AddValue(MakeUnversionedStringValue(*Statistics_.Type(), index.Type));
        }
        if (Statistics_.State()) {
            builder.AddValue(MakeUnversionedStringValue(
                *Statistics_.State(),
                index.TransientState));
        }
        if (Statistics_.StartTime()) {
            builder.AddValue(MakeUnversionedInt64Value(*Statistics_.StartTime(), index.StartTime));
        }
        if (Statistics_.FinishTime()) {
            builder.AddValue(MakeUnversionedInt64Value(*Statistics_.FinishTime(), index.FinishTime));
        }
        if (DefaultLocalAddress_) {
            builder.AddValue(MakeUnversionedStringValue(*DefaultLocalAddress_, index.Address));
        }
        if (Statistics_.Error()) {
            builder.AddValue(MakeUnversionedAnyValue(*Statistics_.Error(), index.Error));
        }
        if (Statistics_.Statistics()) {
            constexpr int Lz4AndBriefStatisticsVersion = 36;
            if (ReportStatisticsLz4_ && archiveVersion >= Lz4AndBriefStatisticsVersion) {
                auto codec = NCompression::GetCodec(NCompression::ECodec::Lz4);
                statisticsLz4 = ToString(codec->Compress(TSharedRef::FromString(*Statistics_.Statistics())));
                builder.AddValue(MakeUnversionedStringValue(statisticsLz4, index.StatisticsLz4));
            } else {
                builder.AddValue(MakeUnversionedAnyValue(*Statistics_.Statistics(), index.Statistics));
            }
            if (archiveVersion >= Lz4AndBriefStatisticsVersion) {
                briefStatisticsYsonString = BuildBriefStatistics(ConvertToNode(TYsonStringBuf(*Statistics_.Statistics())));
                builder.AddValue(MakeUnversionedAnyValue(briefStatisticsYsonString.AsStringBuf(), index.BriefStatistics));
            }
        }
        if (Statistics_.Events()) {
            builder.AddValue(MakeUnversionedAnyValue(*Statistics_.Events(), index.Events));
        }
        if (Statistics_.StderrSize()) {
            builder.AddValue(MakeUnversionedUint64Value(*Statistics_.StderrSize(), index.StderrSize));
        }
        if (archiveVersion >= 31 && Statistics_.CoreInfos()) {
            coreInfosYsonString = ConvertToYsonString(*Statistics_.CoreInfos());
            builder.AddValue(MakeUnversionedAnyValue(coreInfosYsonString.AsStringBuf(), index.CoreInfos));
        }
        if (archiveVersion >= 18) {
            builder.AddValue(MakeUnversionedInt64Value(TInstant::Now().MicroSeconds(), index.UpdateTime));
        }
        if (archiveVersion >= 20 && Statistics_.Spec()) {
            builder.AddValue(MakeUnversionedBooleanValue(Statistics_.Spec().operator bool(), index.HasSpec));
        }
        if (Statistics_.FailContext()) {
            if (archiveVersion >= 23) {
                builder.AddValue(MakeUnversionedUint64Value(Statistics_.FailContext()->size(), index.FailContextSize));
            } else if (archiveVersion >= 21) {
                builder.AddValue(MakeUnversionedBooleanValue(Statistics_.FailContext().operator bool(), index.HasFailContext));
            }
        }
        if (archiveVersion >= 32 && Statistics_.JobCompetitionId()) {
            jobCompetitionIdString = ToString(Statistics_.JobCompetitionId());
            builder.AddValue(MakeUnversionedStringValue(jobCompetitionIdString, index.JobCompetitionId));
        }
        if (archiveVersion >= 33 && Statistics_.HasCompetitors().has_value()) {
            builder.AddValue(MakeUnversionedBooleanValue(Statistics_.HasCompetitors().value(), index.HasCompetitors));
        }
        // COMPAT(gritukan)
        if (archiveVersion >= 34 && Statistics_.ExecAttributes()) {
            builder.AddValue(MakeUnversionedAnyValue(*Statistics_.ExecAttributes(), index.ExecAttributes));
        }
        // COMPAT(gritukan)
        if (archiveVersion >= 35 && Statistics_.TaskName()) {
            builder.AddValue(MakeUnversionedStringValue(*Statistics_.TaskName(), index.TaskName));
        }
        // COMPAT(levysotsky)
        if (archiveVersion >= 37 && Statistics_.TreeId()) {
            builder.AddValue(MakeUnversionedStringValue(*Statistics_.TreeId(), index.PoolTree));
        }
        // COMPAT(levysotsky)
        if (archiveVersion >= 39 && Statistics_.MonitoringDescriptor()) {
            builder.AddValue(MakeUnversionedStringValue(*Statistics_.MonitoringDescriptor(), index.MonitoringDescriptor));
        }

        return builder.FinishRow();
    }

private:
    TJobReport Statistics_;
    bool ReportStatisticsLz4_;
    const std::optional<TString>& DefaultLocalAddress_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationIdRowlet
    : public IArchiveRowlet
{
public:
    TOperationIdRowlet(TJobReport&& statistics)
        : Statistics_(statistics)
    { }

    virtual size_t EstimateSize() const override
    {
        return Statistics_.EstimateSize();
    }

    virtual TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        const auto& index = TOperationIdTableDescriptor::Get().Index;

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[0], index.JobIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[1], index.JobIdLo));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[0], index.OperationIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[1], index.OperationIdLo));

        return builder.FinishRow();
    }

private:
    TJobReport Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobSpecRowlet
    : public IArchiveRowlet
{
public:
    TJobSpecRowlet(TJobReport&& statistics)
        : Statistics_(statistics)
    { }

    virtual size_t EstimateSize() const override
    {
        return Statistics_.EstimateSize();
    }

    virtual TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        const auto& index = TJobSpecTableDescriptor::Get().Index;

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[0], index.JobIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[1], index.JobIdLo));
        if (Statistics_.Spec()) {
            builder.AddValue(MakeUnversionedStringValue(*Statistics_.Spec(), index.Spec));
        }
        if (Statistics_.SpecVersion()) {
            builder.AddValue(MakeUnversionedInt64Value(*Statistics_.SpecVersion(), index.SpecVersion));
        }
        if (Statistics_.Type()) {
            builder.AddValue(MakeUnversionedStringValue(*Statistics_.Type(), index.Type));
        }

        return builder.FinishRow();
    }

private:
    TJobReport Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobStderrRowlet
    : public IArchiveRowlet
{
public:
    TJobStderrRowlet(TJobReport&& statistics)
        : Statistics_(statistics)
    { }

    virtual size_t EstimateSize() const override
    {
        return Statistics_.EstimateSize();
    }

    virtual TUnversionedOwningRow ToRow(int /*archiveVersion*/) const override
    {
        const auto& index = TJobStderrTableDescriptor::Get().Index;

        if (!Statistics_.Stderr()) {
            return {};
        }

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[0], index.OperationIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[1], index.OperationIdLo));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[0], index.JobIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[1], index.JobIdLo));
        builder.AddValue(MakeUnversionedStringValue(*Statistics_.Stderr(), index.Stderr));

        return builder.FinishRow();
    }

private:
    TJobReport Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobFailContextRowlet
    : public IArchiveRowlet
{
public:
    TJobFailContextRowlet(TJobReport&& statistics)
        : Statistics_(statistics)
    { }

    virtual size_t EstimateSize() const override
    {
        return Statistics_.EstimateSize();
    }

    virtual TUnversionedOwningRow ToRow(int archiveVersion) const override
    {
        const auto& index = TJobFailContextTableDescriptor::Get().Index;

        if (archiveVersion < 21 || !Statistics_.FailContext() || Statistics_.FailContext()->size() > MaxStringValueLength) {
            return {};
        }

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[0], index.OperationIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[1], index.OperationIdLo));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[0], index.JobIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[1], index.JobIdLo));
        builder.AddValue(MakeUnversionedStringValue(*Statistics_.FailContext(), index.FailContext));
        
        return builder.FinishRow();
    }

private:
    TJobReport Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobProfileRowlet
    : public IArchiveRowlet
{
public:
    TJobProfileRowlet(TJobReport&& statistics)
        : Statistics_(statistics)
    { }

    virtual size_t EstimateSize() const override
    {
        return Statistics_.EstimateSize();
    }

    virtual TUnversionedOwningRow ToRow(int archiveVersion) const override
    {
        const auto& index = TJobProfileTableDescriptor::Get().Index;
        const auto& profile = Statistics_.Profile();

        if (archiveVersion < 27 || !profile) {
            return {};
        }

        TUnversionedOwningRowBuilder builder;
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[0], index.OperationIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.OperationId().Parts64[1], index.OperationIdLo));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[0], index.JobIdHi));
        builder.AddValue(MakeUnversionedUint64Value(Statistics_.JobId().Parts64[1], index.JobIdLo));
        builder.AddValue(MakeUnversionedInt64Value(0, index.PartIndex));
        builder.AddValue(MakeUnversionedStringValue(profile->Type, index.ProfileType));
        builder.AddValue(MakeUnversionedStringValue(profile->Blob, index.ProfileBlob));
        builder.AddValue(MakeUnversionedDoubleValue(profile->ProfilingProbability, index.ProfilingProbability));
        
        return builder.FinishRow();
    }

private:
    TJobReport Statistics_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TJobReporter::TImpl
    : public TRefCounted
{
public:
    TImpl(
        TJobReporterConfigPtr reporterConfig,
        const NApi::NNative::IConnectionPtr& masterConnection,
        std::optional<TString> localAddress)
        : Client_(
            masterConnection->CreateNativeClient(TClientOptions::FromUser(reporterConfig->User)))
        , Config_(std::move(reporterConfig))
        , LocalAddress_(std::move(localAddress))
        , JobHandler_(
            New<TArchiveReporter>(
                Version_,
                Config_,
                Config_->JobHandler,
                TJobTableDescriptor::Get().NameTable,
                "jobs",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "jobs")))
        , OperationIdHandler_(
            New<TArchiveReporter>(
                Version_,
                Config_,
                Config_->OperationIdHandler,
                TOperationIdTableDescriptor::Get().NameTable,
                "operation_ids",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "operation_ids")))
        , JobSpecHandler_(
            New<TArchiveReporter>(
                Version_,
                Config_,
                Config_->JobSpecHandler,
                TJobSpecTableDescriptor::Get().NameTable,
                "job_specs",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "job_specs")))
        , JobStderrHandler_(
            New<TArchiveReporter>(
                Version_,
                Config_,
                Config_->JobStderrHandler,
                TJobStderrTableDescriptor::Get().NameTable,
                "stderrs",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "stderrs")))
        , JobFailContextHandler_(
            New<TArchiveReporter>(
                Version_,
                Config_,
                Config_->JobFailContextHandler,
                TJobFailContextTableDescriptor::Get().NameTable,
                "fail_contexts",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "fail_contexts")))
        , JobProfileHandler_(
            New<TArchiveReporter>(
                Version_,
                Config_,
                Config_->JobProfileHandler,
                TJobProfileTableDescriptor::Get().NameTable,
                "profiles",
                Client_,
                Reporter_->GetInvoker(),
                ReporterProfiler.WithTag("reporter_type", "profiles")))
    { }

    void HandleJobReport(TJobReport&& jobReport)
    {
        if (IsSpecEntry(jobReport)) {
            JobSpecHandler_->Enqueue(std::make_unique<TJobSpecRowlet>(jobReport.ExtractSpec()));
        }
        if (jobReport.Stderr()) {
            JobStderrHandler_->Enqueue(std::make_unique<TJobStderrRowlet>(jobReport.ExtractStderr()));
        }
        if (jobReport.FailContext()) {
            JobFailContextHandler_->Enqueue(std::make_unique<TJobFailContextRowlet>(jobReport.ExtractFailContext()));
        }
        if (jobReport.Profile()) {
            JobProfileHandler_->Enqueue(std::make_unique<TJobProfileRowlet>(jobReport.ExtractProfile()));
        }
        if (!jobReport.IsEmpty()) {
            OperationIdHandler_->Enqueue(std::make_unique<TOperationIdRowlet>(jobReport.ExtractIds()));
            JobHandler_->Enqueue(std::make_unique<TJobRowlet>(
                std::move(jobReport),
                Config_->ReportStatisticsLz4,
                LocalAddress_));
        }
    }

    void SetEnabled(bool enable)
    {
        JobHandler_->SetEnabled(enable);
        OperationIdHandler_->SetEnabled(enable);
    }

    void SetSpecEnabled(bool enable)
    {
        JobSpecHandler_->SetEnabled(enable);
    }

    void SetStderrEnabled(bool enable)
    {
        JobStderrHandler_->SetEnabled(enable);
    }

    void SetProfileEnabled(bool enable)
    {
        JobProfileHandler_->SetEnabled(enable);
    }

    void SetFailContextEnabled(bool enable)
    {
        JobFailContextHandler_->SetEnabled(enable);
    }

    void SetJobProfileEnabled(bool enable)
    {
        JobProfileHandler_->SetEnabled(enable);
    }

    void SetOperationArchiveVersion(int version)
    {
        Version_->Set(version);
    }

    int ExtractWriteFailuresCount()
    {
        return
            JobHandler_->ExtractWriteFailuresCount() +
            JobSpecHandler_->ExtractWriteFailuresCount() +
            JobStderrHandler_->ExtractWriteFailuresCount() +
            JobFailContextHandler_->ExtractWriteFailuresCount() +
            JobProfileHandler_->ExtractWriteFailuresCount();
    }

    bool GetQueueIsTooLarge()
    {
        return
            JobHandler_->QueueIsTooLarge() ||
            JobSpecHandler_->QueueIsTooLarge() ||
            JobStderrHandler_->QueueIsTooLarge() ||
            JobFailContextHandler_->QueueIsTooLarge() ||
            JobProfileHandler_->QueueIsTooLarge();
    }

private:
    const NNative::IClientPtr Client_;
    const TJobReporterConfigPtr Config_;
    const std::optional<TString> LocalAddress_;
    const TActionQueuePtr Reporter_ = New<TActionQueue>("JobReporter");
    const TArchiveVersionHolderPtr Version_ = New<TArchiveVersionHolder>();
    const TArchiveReporterPtr JobHandler_;
    const TArchiveReporterPtr OperationIdHandler_;
    const TArchiveReporterPtr JobSpecHandler_;
    const TArchiveReporterPtr JobStderrHandler_;
    const TArchiveReporterPtr JobFailContextHandler_;
    const TArchiveReporterPtr JobProfileHandler_;
};

////////////////////////////////////////////////////////////////////////////////

TJobReporter::TJobReporter(
    TJobReporterConfigPtr reporterConfig,
    const NApi::NNative::IConnectionPtr& masterConnection,
    std::optional<TString> localAddress)
    : Impl_(
        reporterConfig->Enabled
            ? New<TImpl>(std::move(reporterConfig), masterConnection, std::move(localAddress))
            : nullptr)
{ }

TJobReporter::~TJobReporter()
{ }

void TJobReporter::HandleJobReport(TJobReport&& jobReport)
{
    if (Impl_) {
        Impl_->HandleJobReport(std::move(jobReport));
    }
}

void TJobReporter::SetEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetEnabled(enable);
    }
}

void TJobReporter::SetSpecEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetSpecEnabled(enable);
    }
}

void TJobReporter::SetStderrEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetStderrEnabled(enable);
    }
}

void TJobReporter::SetProfileEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetProfileEnabled(enable);
    }
}

void TJobReporter::SetFailContextEnabled(bool enable)
{
    if (Impl_) {
        Impl_->SetFailContextEnabled(enable);
    }
}

void TJobReporter::SetOperationArchiveVersion(int version)
{
    if (Impl_) {
        Impl_->SetOperationArchiveVersion(version);
    }
}

int TJobReporter::ExtractWriteFailuresCount()
{
    if (Impl_) {
        return Impl_->ExtractWriteFailuresCount();
    }
    return 0;
}

bool TJobReporter::GetQueueIsTooLarge()
{
    if (Impl_) {
        return Impl_->GetQueueIsTooLarge();
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobAgent
