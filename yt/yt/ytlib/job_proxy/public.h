#pragma once

#include <yt/yt/client/job_proxy/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJobSpecHelper)
DECLARE_REFCOUNTED_STRUCT(IUserJobWriterFactory)
DECLARE_REFCOUNTED_STRUCT(IUserJobReadController)
DECLARE_REFCOUNTED_STRUCT(TJobTestingOptions)

DECLARE_REFCOUNTED_STRUCT(IProfilingMultiChunkReader);
DECLARE_REFCOUNTED_STRUCT(IProfilingMultiChunkWriter);
DECLARE_REFCOUNTED_STRUCT(IProfilingSchemalessFormatWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
