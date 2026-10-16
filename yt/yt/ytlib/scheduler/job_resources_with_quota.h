#pragma once

#include "public.h"
#include "disk_resources.h"
#include "job_resources.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt/core/phoenix/context.h>
#include <yt/yt/core/phoenix/type_decl.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// NB(omgronny): This is an extension of EJobResourceType.
DEFINE_ENUM(EJobResourceWithDiskQuotaType,
    ((UserSlots)    (0))
    ((Cpu)          (1))
    ((Gpu)          (2))
    ((Memory)       (3))
    ((Network)      (4))
    ((DiskQuota)    (5))
);

////////////////////////////////////////////////////////////////////////////////

struct TDiskQuota
{
    NChunkClient::TMediumMap<i64> DiskSpacePerMedium;
    std::optional<i64> DiskSpaceWithoutMedium;

    explicit operator bool() const;

    using TSaveContext = NPhoenix::TSaveContext;
    using TLoadContext = NPhoenix::TLoadContext;

    PHOENIX_DECLARE_TYPE(TDiskQuota, 0x5e9c3a86);
};

TDiskQuota CreateDiskQuota(i32 mediumIndex, i64 diskSpace);
TDiskQuota CreateDiskQuotaWithoutMedium(i64 diskSpace);

TDiskQuota  operator-(const TDiskQuota& quota);

TDiskQuota  operator+(const TDiskQuota& lhs, const TDiskQuota& rhs);
TDiskQuota& operator+=(TDiskQuota& lhs, const TDiskQuota& rhs);

TDiskQuota  operator-(const TDiskQuota& lhs, const TDiskQuota& rhs);
TDiskQuota& operator-=(TDiskQuota& lhs, const TDiskQuota& rhs);

bool operator==(const TDiskQuota& lhs, const TDiskQuota& rhs);

TDiskQuota Max(const TDiskQuota& lhs, const TDiskQuota& rhs);

void Serialize(const TDiskQuota& diskQuota, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TJobResourcesWithQuota
    : public TJobResources
{
public:
    DEFINE_BYREF_RW_PROPERTY(TDiskQuota, DiskQuota);

public:
    TJobResourcesWithQuota() = default;
    TJobResourcesWithQuota(const TJobResources& jobResources);
    TJobResourcesWithQuota(const TJobResources& jobResources, TDiskQuota diskQuota);
    TJobResourcesWithQuota(TJobResourcesWithQuota&&) noexcept = default;
    TJobResourcesWithQuota(const TJobResourcesWithQuota&) = default;

    TJobResourcesWithQuota& operator=(TJobResourcesWithQuota&&) noexcept = default;
    TJobResourcesWithQuota& operator=(const TJobResourcesWithQuota&) = default;

    static TJobResourcesWithQuota Infinite();

    TJobResources ToJobResources() const;
    void SetJobResources(const TJobResources& jobResources);

    using TSaveContext = NPhoenix::TSaveContext;
    using TLoadContext = NPhoenix::TLoadContext;

private:
    PHOENIX_DECLARE_TYPE(TJobResourcesWithQuota, 0x81cd6db9);
};

TJobResourcesWithQuota  operator+(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);
TJobResourcesWithQuota& operator+=(TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

TJobResourcesWithQuota  operator-(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);
TJobResourcesWithQuota& operator-=(TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

TJobResourcesWithQuota Max(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

using TJobResourcesWithQuotaList = TCompactVector<TJobResourcesWithQuota, 8>;

////////////////////////////////////////////////////////////////////////////////

bool Dominates(const TDiskQuota& lhs, const TDiskQuota& rhs);

bool Dominates(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

bool CanSatisfyDiskQuotaRequest(
    const TDiskResources& diskResources,
    TDiskQuota diskQuotaRequest,
    bool considerUsage = true);

bool CanSatisfyDiskQuotaRequests(
    const TDiskResources& diskResources,
    const std::vector<TDiskQuota>& diskQuotaRequests,
    bool considerUsage = true);

// For testing purposes.
bool CanSatisfyDiskQuotaRequests(
    std::vector<i64> availableDiskSpacePerLocation,
    std::vector<i64> diskSpaceRequests);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
