#pragma once

// This header is the first intentionally.
#include <yp/server/lib/misc/public.h>

#include <yp/server/lib/objects/public.h>

#include <array>

namespace NYP::NServer::NCluster {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCluster)
class TAccount;
class TIP4AddressPool;
class TInternetAddress;
class TNetworkModule;
class TNode;
class TNodeSegment;
class TObject;
class TPod;
class TPodDisruptionBudget;
class TPodSet;
class TResource;
class TTopologyZone;

DECLARE_REFCOUNTED_STRUCT(IObjectFilterEvaluator)

template <class T>
class TObjectFilterCache;

DECLARE_REFCOUNTED_STRUCT(IClusterReader)

extern const TString TopologyLabel;

class TAntiaffinityVacancyAllocator;

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MaxResourceDimensions = 3;
using TResourceCapacities = std::array<ui64, MaxResourceDimensions>;

struct TAllocationStatistics;

////////////////////////////////////////////////////////////////////////////////

using NObjects::EObjectType;
using NObjects::EResourceKind;
using NObjects::TObjectId;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NCluster
