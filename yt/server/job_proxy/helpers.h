
#include <yt/ytlib/scheduler/job.pb.h>

#include <yt/ytlib/table_client/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

NScheduler::NProto::TOutputResult GetWrittenChunksBoundaryKeys(
    NTableClient::ISchemalessMultiChunkWriterPtr writer);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
