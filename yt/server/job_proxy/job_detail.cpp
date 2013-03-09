#include "stdafx.h"
#include "job_detail.h"

#include <ytlib/chunk_client/node_directory.cpp>

namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

TJob::TJob(IJobHost* host)
    : Host(host)
{
    YCHECK(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

