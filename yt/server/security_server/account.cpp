#include "stdafx.h"
#include "account.h"

#include <core/ytree/convert.h>
#include <core/ytree/fluent.h>

#include <server/security_server/security_manager.pb.h>

#include <server/cell_master/serialize.h>

namespace NYT {
namespace NSecurityServer {

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

void Serialize(const TAccountStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("resource_usage").Value(statistics.ResourceUsage)
            .Item("committed_resource_usage").Value(statistics.CommittedResourceUsage)
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
    // COMPAT(babenko)
    if (context.GetVersion() >= 200) {
        Load(context, MulticellStatistics_);
    }
    Load(context, ClusterResourceLimits_);
    Load(context, Acd_);
}

TAccountStatistics& TAccount::LocalStatistics()
{
    return *LocalStatisticsPtr_;
}

bool TAccount::IsDiskSpaceLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.DiskSpace > ClusterResourceLimits_.DiskSpace;
}

bool TAccount::IsNodeCountLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.NodeCount > ClusterResourceLimits_.NodeCount;
}

bool TAccount::IsChunkCountLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.ChunkCount > ClusterResourceLimits_.ChunkCount;
}

void TAccount::ValidateResourceUsageIncrease(const TClusterResources& delta)
{
    if (delta.DiskSpace > 0 && ClusterStatistics_.ResourceUsage.DiskSpace + delta.DiskSpace > ClusterResourceLimits_.DiskSpace) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountLimitExceeded,
            "Account %Qv is over disk space limit",
            Name_)
            << TErrorAttribute("usage", ClusterStatistics_.ResourceUsage.DiskSpace)
            << TErrorAttribute("limit", ClusterResourceLimits_.DiskSpace);
    }
    if (delta.NodeCount > 0 && ClusterStatistics_.ResourceUsage.NodeCount + delta.NodeCount > ClusterResourceLimits_.NodeCount) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountLimitExceeded,
            "Account %Qv is over node count limit",
            Name_)
            << TErrorAttribute("usage", ClusterStatistics_.ResourceUsage.NodeCount)
            << TErrorAttribute("limit", ClusterResourceLimits_.NodeCount);
    }
    if (delta.ChunkCount > 0 && ClusterStatistics_.ResourceUsage.ChunkCount + delta.ChunkCount > ClusterResourceLimits_.ChunkCount) {
        THROW_ERROR_EXCEPTION(
            NSecurityClient::EErrorCode::AccountLimitExceeded,
            "Account %Qv is over chunk count limit",
            Name_)
            << TErrorAttribute("usage", ClusterStatistics_.ResourceUsage.ChunkCount)
            << TErrorAttribute("limit", ClusterResourceLimits_.ChunkCount);
    }
}

TAccountStatistics* TAccount::GetCellStatistics(NObjectClient::TCellTag cellTag)
{
    auto it = MulticellStatistics_.find(cellTag);
    YCHECK(it != MulticellStatistics_.end());
    return &it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT

