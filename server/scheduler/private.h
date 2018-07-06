#pragma once

#include "public.h"

#include <yt/core/logging/log.h>

namespace NYP {
namespace NServer {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCluster)
class TObject;
class TPod;
class TNode;
class TTopologyZone;
class TPod;
class TPodSet;
class TNodeSegment;
class TInternetAddress;

template <class T>
class TLabelFilterCache;

class TScheduleQueue;
class TAllocationPlan;

struct TAllocationStatistics;

extern const NYT::NLogging::TLogger Logger;

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NServer
} // namespace NYP
