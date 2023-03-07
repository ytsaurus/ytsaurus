#pragma once

#include "public.h"

#include <yt/ytlib/node_tracker_client/helpers.h>

#include <yt/ytlib/chunk_client/medium_directory.h>

#include <yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/client/chunk_client/public.h>

#include <yt/core/misc/fixed_point_number.h>
#include <yt/core/misc/small_vector.h>

#include <yt/core/profiling/metrics_accumulator.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// For each memory capacity gives the number of nodes with this much memory.
using TMemoryDistribution = THashMap<i64, int>;

// Uses precision of 2 decimal digits.
using TCpuResource = TFixedPointNumber<int, 2>;

// Implementation detail.
class TEmptyJobResourcesBase
{ };

class TExtendedJobResources
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(TCpuResource, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(int, Gpu);
    DEFINE_BYVAL_RW_PROPERTY(i64, JobProxyMemory);
    DEFINE_BYVAL_RW_PROPERTY(i64, UserJobMemory);
    DEFINE_BYVAL_RW_PROPERTY(i64, FootprintMemory);
    DEFINE_BYVAL_RW_PROPERTY(int, Network);

public:
    i64 GetMemory() const;

    void Persist(const TStreamPersistenceContext& context);
};

class TJobResources
    : public TEmptyJobResourcesBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(int, UserSlots);
    DEFINE_BYVAL_RW_PROPERTY(TCpuResource, Cpu);
    DEFINE_BYVAL_RW_PROPERTY(int, Gpu);
    DEFINE_BYVAL_RW_PROPERTY(i64, Memory);
    DEFINE_BYVAL_RW_PROPERTY(int, Network);

public:
    TJobResources() = default;
    TJobResources(const NNodeTrackerClient::NProto::TNodeResources& nodeResources);

    static TJobResources Infinite();

    NNodeTrackerClient::NProto::TNodeResources ToNodeResources() const;

    void Persist(const TStreamPersistenceContext& context);
};

#define ITERATE_JOB_RESOURCES(XX) \
    XX(user_slots,            UserSlots) \
    XX(cpu,                   Cpu) \
    XX(gpu,                   Gpu) \
    XX(user_memory,           Memory) \
    XX(network,               Network)

// NB(antonkikh): Resource types must be numbered from 0 to N - 1.
DEFINE_ENUM(EJobResourceType,
    (UserSlots)
    (Cpu)
    (Gpu)
    (Memory)
    (Network)
);

////////////////////////////////////////////////////////////////////////////////

struct TDiskQuota
{
    SmallDenseMap<int, i64> DiskSpacePerMedium = SmallDenseMap<int, i64>{1};

    void Persist(const TStreamPersistenceContext& context);
};


void FormatValue(TStringBuilderBase* builder, TDiskQuota diskQuota, TStringBuf spec);
TString ToString(TDiskQuota diskQuota);

TDiskQuota CreateDiskQuota(i32 mediumIndex, i64 diskSpace);

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

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits);
TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits,
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);
TString FormatResources(const TJobResources& resources);

[[deprecated("Did you forget to pass medium directory parameter?")]]
TString FormatResources(const TJobResourcesWithQuota& resources);
TString FormatResources(
    const TJobResourcesWithQuota& resources,
    const NChunkClient::TMediumDirectoryPtr& mediumDirectory);
TString FormatResources(const TExtendedJobResources& resources);

void ProfileResources(
    const NProfiling::TProfiler& profiler,
    const TJobResources& resources,
    const TString& prefix = TString(),
    const NProfiling::TTagIdList& tagIds = NProfiling::EmptyTagIds);
void ProfileResources(
    NProfiling::TMetricsAccumulator& accumulator,
    const TJobResources& resources,
    const TString& prefix = TString(),
    const NProfiling::TTagIdList& tagIds = NProfiling::EmptyTagIds);

NNodeTrackerClient::EResourceType GetDominantResource(
    const TJobResources& demand,
    const TJobResources& limits);

double GetDominantResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits);

double GetResource(
    const TJobResources& resources,
    NNodeTrackerClient::EResourceType type);

double GetMinResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator);

double GetMaxResourceRatio(
    const TJobResources& nominator,
    const TJobResources& denominator);

TJobResources GetAdjustedResourceLimits(
    const TJobResources& demand,
    const TJobResources& limits,
    const TMemoryDistribution& execNodeMemoryDistribution);

TJobResources  operator +  (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator -  (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator *  (const TJobResources& lhs, i64 rhs);
TJobResources  operator *  (const TJobResources& lhs, double rhs);
TJobResources& operator *= (TJobResources& lhs, i64 rhs);
TJobResources& operator *= (TJobResources& lhs, double rhs);

TJobResources  operator -  (const TJobResources& resources);

bool operator == (const TJobResources& lhs, const TJobResources& rhs);
bool operator != (const TJobResources& lhs, const TJobResources& rhs);

bool Dominates(const TJobResources& lhs, const TJobResources& rhs);
bool Dominates(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

TJobResources Max(const TJobResources& lhs, const TJobResources& rhs);
TJobResources Min(const TJobResources& lhs, const TJobResources& rhs);
TJobResourcesWithQuota Min(const TJobResourcesWithQuota& lhs, const TJobResourcesWithQuota& rhs);

void Serialize(const TExtendedJobResources& resources, NYson::IYsonConsumer* consumer);
void Serialize(const TJobResources& resources, NYson::IYsonConsumer* consumer);

const TJobResources& MinSpareNodeResources();

bool CanSatisfyDiskQuotaRequest(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    TDiskQuota diskQuotaRequest);

bool CanSatisfyDiskQuotaRequests(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources,
    const std::vector<TDiskQuota>& diskQuotaRequests);

TDiskQuota GetMaxAvailableDiskSpace(
    const NNodeTrackerClient::NProto::TDiskResources& diskResources);

////////////////////////////////////////////////////////////////////////////////

// For testing purposes.
bool CanSatisfyDiskQuotaRequests(
    std::vector<i64> availableDiskSpacePerLocation,
    std::vector<i64> diskSpaceRequests);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NScheduler::NProto::TDiskQuota* protoDiskQuota, const NScheduler::TDiskQuota& diskQuota);
void FromProto(NScheduler::TDiskQuota* diskQuota, const NScheduler::NProto::TDiskQuota& protoDiskQuota);

void ToProto(NScheduler::NProto::TJobResources* protoResources, const NScheduler::TJobResources& resources);
void FromProto(NScheduler::TJobResources* resources, const NScheduler::NProto::TJobResources& protoResources);

void ToProto(NScheduler::NProto::TJobResourcesWithQuota* protoResources, const NScheduler::TJobResourcesWithQuota& resources);
void FromProto(NScheduler::TJobResourcesWithQuota* resources, const NScheduler::NProto::TJobResourcesWithQuota& protoResources);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
