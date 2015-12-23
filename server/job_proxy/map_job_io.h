#pragma once

#include "public.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreateMapJobIO(IJobHost* host);
std::unique_ptr<IUserJobIO> CreateOrderedMapJobIO(IJobHost* host);

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
