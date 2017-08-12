#pragma once

#include "public.h"
#include "user_job_io.h"

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobIOBase
    : public IUserJobIO
{
public:
    explicit TUserJobIOBase(IJobHostPtr host);
    ~TUserJobIOBase();

    virtual void Init() override;

    virtual std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> GetWriters() const override;
    virtual IOutputStream* GetStderrTableWriter() const override;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;
    virtual void PopulateStderrResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;

protected:
    const IJobHostPtr Host_;

    std::atomic<bool> Initialized_ = {false};

    std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> Writers_;
    std::unique_ptr<NTableClient::TBlobTableWriter> StderrTableWriter_;

    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
