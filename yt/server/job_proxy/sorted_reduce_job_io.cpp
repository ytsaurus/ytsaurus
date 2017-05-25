#include "sorted_reduce_job_io.h"
#include "user_job_io_detail.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreateSortedReduceJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TUserJobIOBase(host));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
