#pragma once

#include "public.h"

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

#include <yt/ytlib/scheduler/job.pb.h>
#include <yt/ytlib/scheduler/public.h>

#include <yt/ytlib/table_client/public.h>
#include <yt/ytlib/table_client/schemaful_reader_adapter.h>

class TOutputStream;

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
    virtual NTableClient::ISchemalessMultiChunkReaderPtr GetReader() const = 0;
    virtual TOutputStream* GetStderrTableWriter() const = 0;

    //! Used for key switch injection.
    virtual int GetKeySwitchColumnCount() const = 0;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) = 0;
    virtual void PopulateStderrResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) = 0;

    virtual void CreateReader() = 0;

    virtual NTableClient::TSchemalessReaderFactory GetReaderFactory() = 0;

    virtual void InterruptReader() = 0;

    virtual std::vector<NChunkClient::TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
