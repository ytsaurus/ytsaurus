#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NObjectServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

class TReqCreateForeignObject;
class TReqRemoveForeignObject;
class TReqDestroyObjects;

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TGarbageCollector)

DECLARE_REFCOUNTED_STRUCT(TRequestProfilingCounters)

DECLARE_REFCOUNTED_CLASS(TMutationIdempotizer)

static constexpr int MaxAnnotationLength = 1024;
static constexpr int MaxClusterNameLength = 128;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_LEAKY_GLOBAL(const NLogging::TLogger, ObjectServerLogger, "ObjectServer");
YT_DEFINE_LEAKY_GLOBAL(const NProfiling::TProfiler, ObjectServerProfiler, "/object_server");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
