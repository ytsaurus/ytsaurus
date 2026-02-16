#pragma once

#include "public.h"

namespace NYT::NCellBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TSpareNodesInfo
{
    std::vector<std::string> FreeNodes;
    std::vector<std::string> ScheduledForMaintenance;
    std::vector<std::string> ExternallyDecommissioned;
    THashMap<std::string, std::vector<std::string>> UsedByBundle;
    THashMap<std::string, std::vector<std::string>> ReleasingByBundle;

    std::vector<std::string>& MutableFreeInstances()
    {
        return FreeNodes;
    }

    const std::vector<std::string>& FreeInstances() const
    {
        return FreeNodes;
    }
};

using TPerDataCenterSpareNodesInfo = THashMap<std::string, TSpareNodesInfo>;

////////////////////////////////////////////////////////////////////////////////

struct TSpareProxiesInfo
{
    std::vector<std::string> FreeProxies;
    std::vector<std::string> ScheduledForMaintenance;
    THashMap<std::string, std::vector<std::string>> UsedByBundle;

    std::vector<std::string>& MutableFreeInstances()
    {
        return FreeProxies;
    }

    const std::vector<std::string>& FreeInstances() const
    {
        return FreeProxies;
    }
};

using TPerDataCenterSpareProxiesInfo = THashMap<std::string, TSpareProxiesInfo>;

////////////////////////////////////////////////////////////////////////////////

struct ISpareInstanceAllocator
    : public TRefCounted
{
    virtual std::string Allocate(
        const std::string& zoneName,
        const std::string& dataCenterName,
        const std::string& bundleName) = 0;

    virtual bool HasInstances(
        const std::string& zoneName,
        const std::string& dataCenterName) const = 0;
};

DEFINE_REFCOUNTED_TYPE(ISpareInstanceAllocator)

////////////////////////////////////////////////////////////////////////////////

template <class TSpareInstance>
struct TSpareInstanceAllocator
    : public ISpareInstanceAllocator
{
private:
    using TZoneToDataCenterToInfo = THashMap<std::string, THashMap<std::string, TSpareInstance>>;

public:
    explicit TSpareInstanceAllocator(TZoneToDataCenterToInfo& spareInstances);

    TZoneToDataCenterToInfo& SpareInstances;

    std::string Allocate(
        const std::string& zoneName,
        const std::string& dataCenterName,
        const std::string& bundleName) override;

    bool HasInstances(
        const std::string& zoneName,
        const std::string& dataCenterName) const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellBalancer

#define SPARE_INSTANCES_INL_H_
#include "spare_instances-inl.h"
#undef SPARE_INSTANCES_INL_H_
