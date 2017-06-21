#pragma once

#include "public.h"

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreateMapJobIO(IJobHostPtr host);
std::unique_ptr<IUserJobIO> CreateOrderedMapJobIO(IJobHostPtr host);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
