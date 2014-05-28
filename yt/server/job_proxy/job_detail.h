#pragma once

#include "public.h"
#include "job.h"

#include <ytlib/chunk_client/public.h>

#include <ytlib/job_tracker_client/job.pb.h>
#include <ytlib/job_tracker_client/statistics.h>

#include <ytlib/new_table_client/public.h>

#include <ytlib/scheduler/job.pb.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

//! Base class for all jobs inside job proxy.
class TJob
    : public IJob
{
public:
    explicit TJob(IJobHost* host);

protected:
    TWeakPtr<IJobHost> Host;

    TInstant StartTime;

    TDuration GetElapsedTime() const;

};

////////////////////////////////////////////////////////////////////////////////

class TSimpleJobBase
    : public TJob
{
public:
    explicit TSimpleJobBase(IJobHost *host);

    virtual NJobTrackerClient::NProto::TJobResult Run() override;

    virtual double GetProgress() const override;

    virtual std::vector<NChunkClient::TChunkId> GetFailedChunkIds() const override;

    virtual NJobTrackerClient::NProto::TJobStatistics GetStatistics() const override;

protected:
    const NJobTrackerClient::NProto::TJobSpec& JobSpec_;
    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpecExt_;

    NVersionedTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    NVersionedTableClient::ISchemalessMultiChunkWriterPtr Writer_;

    i64 TotalRowCount_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
