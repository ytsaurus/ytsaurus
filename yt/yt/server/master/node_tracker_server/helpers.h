#pragma once

#include "public.h"

#include <yt/yt/core/misc/id_generator.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T GenerateCounterId(TIdGenerator& generator, T invalidId, T maxId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer

#define HELPERS_INL_H_
#include "helpers-inl.h"
#undef HELPERS_INL_H_
