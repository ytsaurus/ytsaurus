#pragma once

#include "public.h"

#include <yt/yt/orm/client/objects/type.h>
#include <yt/yt/orm/client/objects/private.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

using NClient::NObjects::Logger;

////////////////////////////////////////////////////////////////////////////////

inline const NProfiling::TProfiler Profiler("/objects");

////////////////////////////////////////////////////////////////////////////////

static const NYPath::TYPath EventGenerationSkipAccessControlPath("/access/event_generation_skip_allowed");
static const TString RestoreObjectAccessControlPath("/access/restore_object");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
