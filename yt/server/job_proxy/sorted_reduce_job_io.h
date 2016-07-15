#pragma once

#include "public.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreateSortedReduceJobIO(IJobHostPtr host);
std::unique_ptr<IUserJobIO> CreateJoinReduceJobIO(IJobHostPtr host);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
