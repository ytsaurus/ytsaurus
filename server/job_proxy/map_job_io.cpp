#include "map_job_io.h"
#include "user_job_io_detail.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreateMapJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TUserJobIOBase(host));
}

std::unique_ptr<IUserJobIO> CreateOrderedMapJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TUserJobIOBase(host));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
