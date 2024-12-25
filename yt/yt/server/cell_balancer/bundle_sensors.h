#pragma once

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TBundleSensors final
{
    NProfiling::TProfiler Profiler;

    NProfiling::TGauge CpuAllocated;
    NProfiling::TGauge CpuAlive;
    NProfiling::TGauge CpuQuota;

    NProfiling::TGauge MemoryAllocated;
    NProfiling::TGauge MemoryAlive;
    NProfiling::TGauge MemoryQuota;

    NProfiling::TGauge WriteThreadPoolSize;
    NProfiling::TGauge TabletDynamicSize;
    NProfiling::TGauge TabletStaticSize;
    NProfiling::TGauge CompressedBlockCacheSize;
    NProfiling::TGauge UncompressedBlockCacheSize;
    NProfiling::TGauge KeyFilterBlockCacheSize;
    NProfiling::TGauge VersionedChunkMetaSize;
    NProfiling::TGauge LookupRowCacheSize;
    NProfiling::TGauge QuerySize;

    THashMap<TString, NProfiling::TGauge> AliveNodesBySize;
    THashMap<TString, NProfiling::TGauge> AliveProxiesBySize;

    THashMap<TString, NProfiling::TGauge> AllocatedNodesBySize;
    THashMap<TString, NProfiling::TGauge> AllocatedProxiesBySize;

    THashMap<TString, NProfiling::TGauge> TargetTabletNodeSize;
    THashMap<TString, NProfiling::TGauge> TargetRpcProxSize;

    NProfiling::TGauge UsingSpareNodeCount;
    NProfiling::TGauge UsingSpareProxyCount;

    NProfiling::TGauge AssigningTabletNodes;
    NProfiling::TGauge AssigningSpareNodes;
    NProfiling::TGauge ReleasingSpareNodes;

    NProfiling::TGauge OfflineNodeCount;
    NProfiling::TGauge DecommissionedNodeCount;
    NProfiling::TGauge OfflineProxyCount;
    NProfiling::TGauge MaintenanceRequestedNodeCount;

    NProfiling::TGauge InflightNodeAllocationCount;
    NProfiling::TGauge InflightNodeDeallocationCount;
    NProfiling::TGauge InflightCellRemovalCount;

    NProfiling::TGauge InflightProxyAllocationCounter;
    NProfiling::TGauge InflightProxyDeallocationCounter;

    NProfiling::TTimeGauge NodeAllocationRequestAge;
    NProfiling::TTimeGauge NodeDeallocationRequestAge;
    NProfiling::TTimeGauge RemovingCellsAge;

    NProfiling::TTimeGauge ProxyAllocationRequestAge;
    NProfiling::TTimeGauge ProxyDeallocationRequestAge;

    NProfiling::TTimeGauge BundleCellsDowntime;

    THashMap<TString, NProfiling::TGauge> AssignedBundleNodesPerDC;
};

using TBundleSensorsPtr = TIntrusivePtr<TBundleSensors>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer
