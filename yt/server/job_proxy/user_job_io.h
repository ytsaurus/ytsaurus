#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/ytlib/scheduler/job.pb.h>
#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaful_reader_adapter.h>

class IOutputStream;

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobIO
    : private TNonCopyable
{
    virtual ~IUserJobIO()
    { }

    virtual void Init() = 0;

    virtual std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> GetWriters() const = 0;
    virtual IOutputStream* GetStderrTableWriter() const = 0;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) = 0;
    virtual void PopulateStderrResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
