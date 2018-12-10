#include "account.h"

#include <yt/server/cell_master/serialize.h>

#include <yt/server/security_server/security_manager.pb.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void TAccountStatistics::Persist(NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ResourceUsage);
    Persist(context, CommittedResourceUsage);
}

void ToProto(NProto::TAccountStatistics* protoStatistics, const TAccountStatistics& statistics)
{
    ToProto(protoStatistics->mutable_resource_usage(), statistics.ResourceUsage);
    ToProto(protoStatistics->mutable_committed_resource_usage(), statistics.CommittedResourceUsage);
}

void FromProto(TAccountStatistics* statistics, const NProto::TAccountStatistics& protoStatistics)
{
    FromProto(&statistics->ResourceUsage, protoStatistics.resource_usage());
    FromProto(&statistics->CommittedResourceUsage, protoStatistics.committed_resource_usage());
}

void Serialize(const TAccountStatistics& statistics, IYsonConsumer* consumer, const NChunkServer::TChunkManagerPtr& chunkManager)
{
    auto usage = New<TSerializableClusterResources>(chunkManager, statistics.ResourceUsage);
    auto committedUsage = New<TSerializableClusterResources>(chunkManager, statistics.CommittedResourceUsage);

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("resource_usage").Value(usage)
            .Item("committed_resource_usage").Value(committedUsage)
        .EndMap();
}

TAccountStatistics& operator += (TAccountStatistics& lhs, const TAccountStatistics& rhs)
{
    lhs.ResourceUsage += rhs.ResourceUsage;
    lhs.CommittedResourceUsage += rhs.CommittedResourceUsage;
    return lhs;
}

TAccountStatistics operator + (const TAccountStatistics& lhs, const TAccountStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(const TAccountId& id)
    : TNonversionedObjectBase(id)
    , LocalStatisticsPtr_(nullptr)
    , Acd_(this)
{ }

void TAccount::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedObjectBase::Save(context);

    using NYT::Save;
    Save(context, Name_);
    Save(context, ClusterStatistics_);
    Save(context, MulticellStatistics_);
    Save(context, ClusterResourceLimits_);
    Save(context, Acd_);
}

void TAccount::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedObjectBase::Load(context);

    using NYT::Load;
    Load(context, Name_);
    Load(context, ClusterStatistics_);
    Load(context, MulticellStatistics_);
    Load(context, ClusterResourceLimits_);
    Load(context, Acd_);
}

TAccountStatistics& TAccount::LocalStatistics()
{
    return *LocalStatisticsPtr_;
}

bool TAccount::IsDiskSpaceLimitViolated() const
{
    const auto& usage = ClusterStatistics_.ResourceUsage.DiskSpace;
    const auto& limits = ClusterResourceLimits_.DiskSpace;

    return !std::equal(
        std::begin(usage),
        std::end(usage),
        std::begin(limits),
        std::less_equal<i64>());
}

bool TAccount::IsDiskSpaceLimitViolated(int mediumIndex) const
{
    const auto& usage = ClusterStatistics_.ResourceUsage.DiskSpace;
    const auto& limits = ClusterResourceLimits_.DiskSpace;
    return usage[mediumIndex] > limits[mediumIndex];
}

bool TAccount::IsNodeCountLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.NodeCount > ClusterResourceLimits_.NodeCount;
}

bool TAccount::IsChunkCountLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.ChunkCount > ClusterResourceLimits_.ChunkCount;
}

bool TAccount::IsTabletCountLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.TabletCount > ClusterResourceLimits_.TabletCount;
}

bool TAccount::IsTabletStaticMemoryLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.TabletStaticMemory > ClusterResourceLimits_.TabletStaticMemory;
}

TAccountStatistics* TAccount::GetCellStatistics(NObjectClient::TCellTag cellTag)
{
    auto it = MulticellStatistics_.find(cellTag);
    YCHECK(it != MulticellStatistics_.end());
    return &it->second;
}

void TAccount::RecomputeClusterStatistics()
{
    ClusterStatistics_ = TAccountStatistics();
    for (const auto& pair : MulticellStatistics_) {
        ClusterStatistics_ += pair.second;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer

