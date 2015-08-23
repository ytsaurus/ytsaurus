#pragma once

#include "public.h"

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/schemaful_reader_adapter.h>

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobIO
    : private TNonCopyable
{
    virtual void Init() = 0;

    virtual const std::vector<NTableClient::ISchemalessMultiChunkWriterPtr>& GetWriters() const = 0;
    virtual const NTableClient::ISchemalessMultiChunkReaderPtr& GetReader() const = 0;

    //! Used for key switch injection.
    virtual int GetReduceKeyColumnCount() const = 0;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) = 0;

    virtual void CreateReader() = 0;

    virtual NTableClient::TSchemalessReaderFactory GetReaderFactory() = 0;

    virtual ~IUserJobIO()
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT


