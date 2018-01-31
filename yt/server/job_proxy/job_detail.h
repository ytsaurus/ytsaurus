#pragma once

#include "public.h"
#include "job.h"

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/job_tracker_client/job.pb.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaful_reader_adapter.h>
#include <yt/ytlib/table_client/schemaful_writer_adapter.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void RunQuery(
    const NScheduler::NProto::TQuerySpec& querySpec,
    const NTableClient::TSchemalessReaderFactory& readerFactory,
    const NTableClient::TSchemalessWriterFactory& writerFactory);

////////////////////////////////////////////////////////////////////////////////

//! Base class for all jobs inside job proxy.
class TJob
    : public IJob
{
public:
    explicit TJob(IJobHostPtr host);

    virtual std::vector<NChunkClient::TChunkId> DumpInputContext() override;
    virtual TString GetStderr() override;
    virtual NYson::TYsonString StraceJob() override;
    virtual void SignalJob(const TString& signalName) override;
    virtual NYson::TYsonString PollJobShell(const NYson::TYsonString& parameters) override;
    virtual void Fail() override;

protected:
    const IJobHostPtr Host_;
    const TInstant StartTime_;
};

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobBase
    : public TJob
{
public:
    explicit TSimpleJobBase(IJobHostPtr host);

    virtual NJobTrackerClient::NProto::TJobResult Run() override;

    virtual void Cleanup() override;

    virtual double GetProgress() const override;

    virtual ui64 GetStderrSize() const override;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;
    virtual NChunkClient::TInterruptDescriptor GetInterruptDescriptor() const override;

    virtual NJobTrackerClient::TStatistics GetStatistics() const override;

    virtual bool ShouldSendBoundaryKeys() const;

    virtual void Interrupt() override;

protected:
    const NJobTrackerClient::NProto::TJobSpec& JobSpec_;
    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpecExt_;

    NTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    NTableClient::ISchemalessMultiChunkWriterPtr Writer_;
    NTableClient::TSchemalessReaderFactory ReaderFactory_;
    NTableClient::TSchemalessWriterFactory WriterFactory_;

    i64 TotalRowCount_ = 0;

    std::atomic<bool> Initialized_ = {false};
    std::atomic<bool> Interrupted_ = {false};

    virtual void CreateReader() = 0;
    virtual void CreateWriter() = 0;

    NTableClient::TTableWriterConfigPtr GetWriterConfig(const NScheduler::NProto::TTableOutputSpec& outputSpec);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
