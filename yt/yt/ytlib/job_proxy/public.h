#pragma once

#include <yt/yt/client/job_proxy/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IJobSpecHelper)
DECLARE_REFCOUNTED_STRUCT(IUserJobWriterFactory)
DECLARE_REFCOUNTED_STRUCT(IUserJobReadController)
DECLARE_REFCOUNTED_CLASS(TJobTestingOptions)

DECLARE_REFCOUNTED_CLASS(IProfilingMultiChunkReader);
DECLARE_REFCOUNTED_CLASS(IProfilingMultiChunkWriter);
DECLARE_REFCOUNTED_CLASS(IProfilingSchemalessFormatWriter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
