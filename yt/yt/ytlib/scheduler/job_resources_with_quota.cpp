#include "job_resources_with_quota.h"
#include "job_resources_helpers.h"
#include "config.h"

#include <yt/yt/ytlib/node_tracker_client/helpers.h>

#include <yt/yt/ytlib/scheduler/proto/resources.pb.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NScheduler {

using namespace NYson;
using namespace NYTree;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TDiskQuota::operator bool() const
{
    return !DiskSpacePerMedium.empty() || DiskSpaceWithoutMedium;
}

void TDiskQuota::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, DiskSpacePerMedium);
    Persist(context, DiskSpaceWithoutMedium);
}

TDiskQuota CreateDiskQuota(i32 mediumIndex, i64 diskSpace)
{
    TDiskQuota result;
    result.DiskSpacePerMedium.emplace(mediumIndex, diskSpace);
    return result;
}

TDiskQuota CreateDiskQuotaWithoutMedium(i64 diskSpace)
{
    TDiskQuota result;
    result.DiskSpaceWithoutMedium = diskSpace;
    return result;
}

TDiskQuota  operator - (const TDiskQuota& quota)
{
    TDiskQuota result;
    if (quota.DiskSpaceWithoutMedium) {
        result.DiskSpaceWithoutMedium = -*quota.DiskSpaceWithoutMedium;
    }
    for (auto [key, value] : quota.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] = -value;
    }
    return result;
}

TDiskQuota  operator + (const TDiskQuota& lhs, const TDiskQuota& rhs)
{
    TDiskQuota result;
    result.DiskSpaceWithoutMedium = lhs.DiskSpaceWithoutMedium.value_or(0) + rhs.DiskSpaceWithoutMedium.value_or(0);
    if (*result.DiskSpaceWithoutMedium == 0) {
        result.DiskSpaceWithoutMedium = std::nullopt;
    }
    for (auto [key, value] : lhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] += value;
    }
    for (auto [key, value] : rhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] += value;
    }
    return result;
}

TDiskQuota& operator += (TDiskQuota& lhs, const TDiskQuota& rhs)
{
    lhs = lhs + rhs;
    return lhs;
}

TDiskQuota  operator - (const TDiskQuota& lhs, const TDiskQuota& rhs)
{
    TDiskQuota result;
    result.DiskSpaceWithoutMedium = lhs.DiskSpaceWithoutMedium.value_or(0) - rhs.DiskSpaceWithoutMedium.value_or(0);
    if (*result.DiskSpaceWithoutMedium == 0) {
        result.DiskSpaceWithoutMedium = std::nullopt;
    }
    for (auto [key, value] : lhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] += value;
    }
    for (auto [key, value] : rhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] -= value;
    }
    return result;
}

TDiskQuota& operator -= (TDiskQuota& lhs, const TDiskQuota& rhs)
{
    lhs = lhs - rhs;
    return lhs;
}

bool operator==(const TDiskQuota& lhs, const TDiskQuota& rhs)
{
    if (lhs.DiskSpacePerMedium.size() != rhs.DiskSpacePerMedium.size()) {
        return false;
    }

    for (auto [key, value] : lhs.DiskSpacePerMedium) {
        auto it = rhs.DiskSpacePerMedium.find(key);
        if (it == rhs.DiskSpacePerMedium.end() || it->second != value) {
            return false;
        }
    }

    return lhs.DiskSpaceWithoutMedium == rhs.DiskSpaceWithoutMedium;
}

TDiskQuota Max(const TDiskQuota& lhs, const TDiskQuota& rhs)
{
    TDiskQuota result;
    result.DiskSpaceWithoutMedium = std::max(lhs.DiskSpaceWithoutMedium.value_or(0), rhs.DiskSpaceWithoutMedium.value_or(0));
    if (*result.DiskSpaceWithoutMedium == 0) {
        result.DiskSpaceWithoutMedium = std::nullopt;
    }
    for (auto [key, value] : lhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] = std::max(result.DiskSpacePerMedium[key], value);
    }
    for (auto [key, value] : rhs.DiskSpacePerMedium) {
        result.DiskSpacePerMedium[key] = std::max(result.DiskSpacePerMedium[key], value);
    }
    return result;
}

void Serialize(const TDiskQuota& diskQuota, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("disk_space_per_medium").Value(diskQuota.DiskSpacePerMedium)
            .Item("disk_space_without_medium").Value(diskQuota.DiskSpaceWithoutMedium)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TJobResourcesWithQuota::TJobResourcesWithQuota(const TJobResources& jobResources)
    : TJobResources(jobResources)
{ }

TJobResourcesWithQuota::TJobResourcesWithQuota(const TJobResources& jobResources, TDiskQuota diskQuota)
    : TJobResources(jobResources)
    , DiskQuota_(std::move(diskQuota))
{ }

TJobResourcesWithQuota TJobResourcesWithQuota::Infinite()
{
    return TJobResourcesWithQuota(TJobResources::Infinite());
}

TJobResources TJobResourcesWithQuota::ToJobResources() const
{
    return *this;
}

void TJobResourcesWithQuota::SetJobResources(const TJobResources& jobResources)
{
    #define XX(name, Name) Set##Name(jobResources.Get##Name());
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void TJobResourcesWithQuota::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, *static_cast<TJobResources*>(this));
    Persist(context, DiskQuota_);
}

TJobResourcesWithQuota  operator + (const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    TJobResourcesWithQuota result = lhs.ToJobResources() + rhs.ToJobResources();
    result.DiskQuota() = lhs.DiskQuota() + rhs.DiskQuota();
    return result;
}

TJobResourcesWithQuota& operator += (TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    lhs = lhs + rhs;
    return lhs;
}

TJobResourcesWithQuota  operator - (const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    TJobResourcesWithQuota result = lhs.ToJobResources() - rhs.ToJobResources();
    result.DiskQuota() = lhs.DiskQuota() - rhs.DiskQuota();
    return result;
}

TJobResourcesWithQuota& operator -= (TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    lhs = lhs - rhs;
    return lhs;
}

TJobResourcesWithQuota Max(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    TJobResourcesWithQuota result = Max(lhs.ToJobResources(), rhs.ToJobResources());
    result.DiskQuota() = Max(lhs.DiskQuota(), rhs.DiskQuota());
    return result;
}

////////////////////////////////////////////////////////////////////////////////

bool Dominates(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs)
{
    bool result =
    #define XX(name, Name) lhs.Get##Name() >= rhs.Get##Name() &&
        ITERATE_JOB_RESOURCES(XX)
    #undef XX
    true;
    auto rhsDiskQuota = rhs.DiskQuota();
    for (auto [mediumIndex, diskSpace] : lhs.DiskQuota().DiskSpacePerMedium) {
        auto it = rhsDiskQuota.DiskSpacePerMedium.find(mediumIndex);
        if (it != rhsDiskQuota.DiskSpacePerMedium.end() && diskSpace < it->second) {
            return false;
        }
    }
    return result;
}

bool CanSatisfyDiskQuotaRequests(
    std::vector<i64> availableDiskSpacePerLocation,
    std::vector<i64> diskSpaceRequests)
{
    std::sort(availableDiskSpacePerLocation.begin(), availableDiskSpacePerLocation.end());
    std::sort(diskSpaceRequests.begin(), diskSpaceRequests.end(), std::greater<i64>());

    for (i64 diskSpace : diskSpaceRequests) {
        auto it = std::lower_bound(availableDiskSpacePerLocation.begin(), availableDiskSpacePerLocation.end(), diskSpace);
        if (it == availableDiskSpacePerLocation.end()) {
            return false;
        }
        *it -= diskSpace;
        while (it != availableDiskSpacePerLocation.begin() && *it < *(it - 1)) {
            std::swap(*it, *(it - 1));
            --it;
        }
    }

    return true;
}

bool CanSatisfyDiskQuotaRequest(
    const std::vector<i64>& availableDiskSpacePerLocation,
    i64 diskSpaceRequest)
{
    for (i64 availableDiskSpace : availableDiskSpacePerLocation) {
        if (diskSpaceRequest <= availableDiskSpace) {
            return true;
        }
    }
    return false;
}

bool HasLocationWithDefaultMedium(const TDiskResources& diskResources)
{
    bool hasLocationWithDefaultMedium = false;
    for (const auto& diskLocationResources : diskResources.DiskLocationResources) {
        if (diskLocationResources.MediumIndex == diskResources.DefaultMediumIndex) {
            hasLocationWithDefaultMedium = true;
        }
    }
    return hasLocationWithDefaultMedium;
}

bool CanSatisfyDiskQuotaRequest(
    const TDiskResources& diskResources,
    TDiskQuota diskQuotaRequest,
    bool considerUsage)
{
    THashMap<int, std::vector<i64>> availableDiskSpacePerMedium;
    for (const auto& diskLocationResources : diskResources.DiskLocationResources) {
        availableDiskSpacePerMedium[diskLocationResources.MediumIndex].push_back(
            considerUsage
                ? diskLocationResources.Limit - diskLocationResources.Usage
                : diskLocationResources.Limit);
    }
    for (auto [mediumIndex, diskSpace] : diskQuotaRequest.DiskSpacePerMedium) {
        if (!CanSatisfyDiskQuotaRequest(availableDiskSpacePerMedium[mediumIndex], diskSpace)) {
            return false;
        }
    }
    if (diskQuotaRequest.DiskSpaceWithoutMedium &&
        !CanSatisfyDiskQuotaRequest(
            availableDiskSpacePerMedium[diskResources.DefaultMediumIndex],
            *diskQuotaRequest.DiskSpaceWithoutMedium))
    {
        return false;
    }

    if (!diskQuotaRequest && !HasLocationWithDefaultMedium(diskResources)) {
        return false;
    }

    return true;
}

bool CanSatisfyDiskQuotaRequests(
    const TDiskResources& diskResources,
    const std::vector<TDiskQuota>& diskQuotaRequests,
    bool considerUsage)
{
    THashMap<int, std::vector<i64>> availableDiskSpacePerMedium;
    for (const auto& diskLocationResources : diskResources.DiskLocationResources) {
        availableDiskSpacePerMedium[diskLocationResources.MediumIndex].push_back(
            considerUsage
                ? diskLocationResources.Limit - diskLocationResources.Usage
                : diskLocationResources.Limit);
    }

    THashMap<int, std::vector<i64>> diskSpaceRequestsPerMedium;
    bool hasEmptyDiskRequest = false;
    for (const auto& diskQuotaRequest : diskQuotaRequests) {
        for (auto [mediumIndex, diskSpace] : diskQuotaRequest.DiskSpacePerMedium) {
            diskSpaceRequestsPerMedium[mediumIndex].push_back(diskSpace);
        }
        if (diskQuotaRequest.DiskSpaceWithoutMedium) {
            diskSpaceRequestsPerMedium[diskResources.DefaultMediumIndex].push_back(*diskQuotaRequest.DiskSpaceWithoutMedium);
        }
        if (!diskQuotaRequest && !diskQuotaRequest.DiskSpaceWithoutMedium) {
            hasEmptyDiskRequest = true;
        }
    }

    if (hasEmptyDiskRequest && !HasLocationWithDefaultMedium(diskResources)) {
        return false;
    }

    for (const auto& [mediumIndex, diskSpaceRequests] : diskSpaceRequestsPerMedium) {
        if (!CanSatisfyDiskQuotaRequests(availableDiskSpacePerMedium[mediumIndex], diskSpaceRequests)) {
            return false;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
