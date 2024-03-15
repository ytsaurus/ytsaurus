#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/node_tracker_client/proto/node.pb.h>

#include <yt/yt/library/profiling/producer.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/core/ytree/ypath_service.h>

#include <yt/yt/library/containers/public.h>

#include <library/cpp/yt/containers/enum_indexed_array.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

struct ISlot
    : public TRefCounted
{
    virtual void ResetState() = 0;
};

DEFINE_REFCOUNTED_TYPE(ISlot)

////////////////////////////////////////////////////////////////////////////////

#define ITERATE_JOB_RESOURCE_PROTO_FIELDS(XX) \
    XX(cpu,                   Cpu) \
    XX(vcpu,                  VCpu) \
    XX(gpu,                   Gpu) \
    XX(network,               Network) \
    XX(system_memory,         SystemMemory) \
    XX(user_memory,           UserMemory) \
    XX(user_slots,            UserSlots) \
    XX(replication_slots,     ReplicationSlots) \
    XX(removal_slots,         RemovalSlots) \
    XX(repair_slots,          RepairSlots) \
    XX(seal_slots,            SealSlots) \
    XX(merge_slots,           MergeSlots) \
    XX(autotomy_slots,        AutotomySlots) \
    XX(reincarnation_slots,   ReincarnationSlots) \
    XX(replication_data_size, ReplicationDataSize) \
    XX(repair_data_size,      RepairDataSize) \
    XX(merge_data_size,       MergeDataSize)

////////////////////////////////////////////////////////////////////////////////

#define ITERATE_JOB_RESOURCE_FIELDS(XX) \
    ITERATE_JOB_RESOURCE_PROTO_FIELDS(XX) \
    XX(disk_space_request,    DiskSpaceRequest)

////////////////////////////////////////////////////////////////////////////////

struct TJobResources
{
    double Cpu = 0.0;
    double VCpu = 0.0;

    i32 Gpu = 0;
    i32 Network = 0;

    i64 SystemMemory = 0;
    i64 UserMemory = 0;

    i64 DiskSpaceRequest = 0;
    i64 InodeRequest = 0;

    i32 UserSlots = 0;

    i32 ReplicationSlots = 0;
    i32 RemovalSlots = 0;
    i32 RepairSlots = 0;
    i32 SealSlots = 0;
    i32 MergeSlots = 0;
    i32 AutotomySlots = 0;
    i32 ReincarnationSlots = 0;

    i64 ReplicationDataSize = 0;
    i64 RepairDataSize = 0;
    i64 MergeDataSize = 0;
};

TString FormatResourceUsage(
    const TJobResources& usage,
    const TJobResources& limits);
TString FormatResources(const TJobResources& resources);

NNodeTrackerClient::NProto::TNodeResources ToNodeResources(const TJobResources& jobResources);
TJobResources FromNodeResources(const NNodeTrackerClient::NProto::TNodeResources& jobResources);

void ProfileResources(NProfiling::ISensorWriter* writer, const TJobResources& resources);

const TJobResources& ZeroJobResources();
const TJobResources& InfiniteJobResources();

TJobResources  operator + (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator += (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator - (const TJobResources& lhs, const TJobResources& rhs);
TJobResources& operator -= (TJobResources& lhs, const TJobResources& rhs);

TJobResources  operator * (const TJobResources& lhs, i64 rhs);
TJobResources  operator * (const TJobResources& lhs, double rhs);
TJobResources& operator *= (TJobResources& lhs, i64 rhs);
TJobResources& operator *= (TJobResources& lhs, double rhs);

TJobResources  operator - (const TJobResources& resources);

bool operator == (const TJobResources& lhs, const TJobResources& rhs);

TJobResources MakeNonnegative(const TJobResources& resources);
bool Dominates(const TJobResources& lhs, const TJobResources& rhs);

TJobResources Max(const TJobResources& a, const TJobResources& b);
TJobResources Min(const TJobResources& a, const TJobResources& b);

void Serialize(
    const TJobResources& resources,
    NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TJobResourceAttributes
{
    bool AllowIdleCpuPolicy = false;

    std::optional<TString> CudaToolkitVersion;

    // TODO(pogorelov): MediumIndex should be provided by disk request in job resources, and should not be placed here.
    std::optional<i64> MediumIndex;
};

////////////////////////////////////////////////////////////////////////////////

class TNodeResourceManager
    : public TRefCounted
{
public:
    explicit TNodeResourceManager(IBootstrap* bootstrap);

    void Start();

    /*!
    *  \note
    *  Thread affinity: any
    */
    std::optional<double> GetCpuGuarantee() const;
    std::optional<double> GetCpuLimit() const;
    double GetJobsCpuLimit() const;
    double GetTabletSlotCpu() const;
    double GetNodeDedicatedCpu() const;

    double GetCpuUsage() const;
    i64 GetMemoryUsage() const;

    double GetCpuDemand() const;
    i64 GetMemoryDemand() const;

    std::optional<i64> GetNetTxLimit() const;
    std::optional<i64> GetNetRxLimit() const;

    // TODO(gritukan): Drop it in favour of dynamic config.
    void SetResourceLimitsOverride(const NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides& resourceLimitsOverride);

    void OnInstanceLimitsUpdated(const NContainers::TInstanceLimits& limits);

    NYTree::IYPathServicePtr GetOrchidService();

    DEFINE_SIGNAL(void(), JobsCpuLimitUpdated);

    DEFINE_SIGNAL(void(i64), SelfMemoryGuaranteeUpdated);

private:
    IBootstrap* const Bootstrap_;

    const NConcurrency::TPeriodicExecutorPtr UpdateExecutor_;

    TAtomicObject<NContainers::TInstanceLimits> Limits_;

    i64 SelfMemoryGuarantee_ = 0;
    std::atomic<double> JobsCpuLimit_ = 0;

    NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides ResourceLimitsOverride_;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

    void UpdateLimits();
    void UpdateMemoryLimits();
    void UpdateMemoryFootprint();
    void UpdateJobsCpuLimit();

    NNodeTrackerClient::NProto::TNodeResourceLimitsOverrides ComputeEffectiveResourceLimitsOverrides() const;

    TJobResources GetJobResourceUsage() const;

    void BuildOrchid(NYson::IYsonConsumer* consumer) const;

    TEnumIndexedArray<EMemoryCategory, TMemoryLimitPtr> GetMemoryLimits() const;
};

DEFINE_REFCOUNTED_TYPE(TNodeResourceManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
