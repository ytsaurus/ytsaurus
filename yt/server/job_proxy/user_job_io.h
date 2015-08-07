#pragma once

#include "public.h"

#include <ytlib/new_table_client/public.h>

#include <ytlib/scheduler/job.pb.h>
#include <ytlib/scheduler/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobIO
    : private TNonCopyable
{
    virtual void Init() = 0;

    virtual const std::vector<NVersionedTableClient::ISchemalessMultiChunkWriterPtr>& GetWriters() const = 0;
    virtual const std::vector<NVersionedTableClient::ISchemalessMultiChunkReaderPtr>& GetReaders() const = 0;

    //! Used for key switch injection.
    virtual int GetReduceKeyColumnCount() const = 0;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) = 0;

    virtual ~IUserJobIO()
    { }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT


