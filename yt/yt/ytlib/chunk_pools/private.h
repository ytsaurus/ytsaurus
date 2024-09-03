#pragma once

#include "public.h"

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/ytlib/controller_agent/persistence.h>

namespace NYT::NChunkPools {

using NControllerAgent::TLoadContext;
using NControllerAgent::TSaveContext;
using NControllerAgent::TPersistenceContext;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOutputOrder)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

