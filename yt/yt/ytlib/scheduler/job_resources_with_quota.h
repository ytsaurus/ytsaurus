#pragma once

#include "public.h"
#include "job_resources.h"

#include <yt/yt/ytlib/chunk_client/medium_directory.h>

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/core/misc/small_vector.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TDiskQuota
{
    SmallDenseMap<int, i64> DiskSpacePerMedium = SmallDenseMap<int, i64>{1};

    std::optional<i64> DiskSpaceWithoutMedium;

    operator bool() const;

    void Persist(const TStreamPersistenceContext& context);
};


TDiskQuota CreateDiskQuota(i32 mediumIndex, i64 diskSpace);
TDiskQuota CreateDiskQuotaWithoutMedium(i64 diskSpace);

TDiskQuota  operator -  (const TDiskQuota& quota);

TDiskQuota  operator +  (const TDiskQuota& lhs, const TDiskQuota& rhs);
TDiskQuota& operator += (TDiskQuota& lhs, const TDiskQuota& rhs);

TDiskQuota  operator -  (const TDiskQuota& lhs, const TDiskQuota& rhs);
TDiskQuota& operator -= (TDiskQuota& lhs, const TDiskQuota& rhs);

////////////////////////////////////////////////////////////////////////////////

class TJobResourcesWithQuota
    : public TJobResources
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TDiskQuota, DiskQuota)

public:
    TJobResourcesWithQuota() = default;
    TJobResourcesWithQuota(const TJobResources& jobResources);

    static TJobResourcesWithQuota Infinite();

    TJobResources ToJobResources() const;

    void Persist(const TStreamPersistenceContext& context);
};

using TJobResourcesWithQuotaList = SmallVector<TJobResourcesWithQuota, 8>;

////////////////////////////////////////////////////////////////////////////////

bool Dominates(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

bool CanSatisfyDiskQuotaRequest(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    TDiskQuota diskQuotaRequest);

bool CanSatisfyDiskQuotaRequests(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const std::vector<TDiskQuota>& diskQuotaRequests);

// For testing purposes.
bool CanSatisfyDiskQuotaRequests(
    std::vector<i64> availableDiskSpacePerLocation,
    std::vector<i64> diskSpaceRequests);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
