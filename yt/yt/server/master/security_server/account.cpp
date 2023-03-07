#include "account.h"

#include <yt/server/master/cell_master/serialize.h>

#include <yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/core/ytree/convert.h>
#include <yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectServer;

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

TAccount::TAccount(TAccountId id, bool isRoot)
    : TNonversionedMapObjectBase<TAccount>(id, isRoot)
{ }

TString TAccount::GetLowercaseObjectName() const
{
    return Format("account %Qv", GetName());
}

TString TAccount::GetCapitalizedObjectName() const
{
    return Format("Account %Qv", GetName());
}

TString TAccount::GetRootName() const
{
    YT_VERIFY(IsRoot());
    return NSecurityClient::RootAccountName;
}

void TAccount::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedMapObjectBase<TAccount>::Save(context);

    using NYT::Save;
    Save(context, ClusterStatistics_);
    Save(context, MulticellStatistics_);
    Save(context, ClusterResourceLimits_);
    Save(context, AllowChildrenLimitOvercommit_);
}

void TAccount::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;

    // COMPAT(kiselyovp)
    if (context.GetVersion() < EMasterReign::HierarchicalAccounts) {
        TNonversionedObjectBase::Load(context);

        Load(context, LegacyName_);
        Load(context, ClusterStatistics_);
        Load(context, MulticellStatistics_);
        Load(context, ClusterResourceLimits_);
        Load(context, Acd_);
        AllowChildrenLimitOvercommit_ = false;
    } else {
        TNonversionedMapObjectBase<TAccount>::Load(context);

        Load(context, ClusterStatistics_);
        Load(context, MulticellStatistics_);
        Load(context, ClusterResourceLimits_);
        Load(context, AllowChildrenLimitOvercommit_);
    }
}

TAccountStatistics& TAccount::LocalStatistics()
{
    return *LocalStatisticsPtr_;
}

bool TAccount::IsDiskSpaceLimitViolated() const
{
    const auto& usage = ClusterStatistics_.ResourceUsage.DiskSpace();

    for (const auto& [mediumIndex, diskSpace] : usage) {
        if (diskSpace > ClusterResourceLimits_.DiskSpace().lookup(mediumIndex)) {
            return true;
        }
    }

    return false;
}

bool TAccount::IsDiskSpaceLimitViolated(int mediumIndex) const
{
    const auto& usage = ClusterStatistics_.ResourceUsage.DiskSpace();
    auto limit = ClusterResourceLimits_.DiskSpace().lookup(mediumIndex);
    return usage.lookup(mediumIndex) > limit;
}

bool TAccount::IsNodeCountLimitViolated() const
{
    // See TSecurityManager::ValidateResourceUsageIncrease for the reason why committed usage is compared here.
    return ClusterStatistics_.CommittedResourceUsage.NodeCount > ClusterResourceLimits_.NodeCount;
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

bool TAccount::IsMasterMemoryLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.MasterMemory > ClusterResourceLimits_.MasterMemory;
}

TAccountStatistics* TAccount::GetCellStatistics(NObjectClient::TCellTag cellTag)
{
    return &GetOrCrash(MulticellStatistics_, cellTag);
}

void TAccount::RecomputeClusterStatistics()
{
    ClusterStatistics_ = TAccountStatistics();
    for (const auto& pair : MulticellStatistics_) {
        ClusterStatistics_ += pair.second;
    }
}

void TAccount::AttachChild(const TString& key, TAccount* child) noexcept
{
    TNonversionedMapObjectBase<TAccount>::AttachChild(key, child);

    const auto& childLocalResourceUsage = child->LocalStatistics().ResourceUsage;
    const auto& childLocalCommittedResourceUsage = child->LocalStatistics().CommittedResourceUsage;

    const auto& childResourceUsage = child->ClusterStatistics().ResourceUsage;
    const auto& childCommittedResourceUsage = child->ClusterStatistics().CommittedResourceUsage;

    for (auto* account = this; account; account = account->GetParent()) {
        auto& localStatistics = account->LocalStatistics();
        auto& clusterStatistics = account->ClusterStatistics();

        localStatistics.ResourceUsage += childLocalResourceUsage;
        clusterStatistics.ResourceUsage += childResourceUsage;

        localStatistics.CommittedResourceUsage += childLocalCommittedResourceUsage;
        clusterStatistics.CommittedResourceUsage += childCommittedResourceUsage;
    }
}

void TAccount::DetachChild(TAccount* child) noexcept
{
    TNonversionedMapObjectBase<TAccount>::DetachChild(child);

    const auto& childLocalResourceUsage = child->LocalStatistics().ResourceUsage;
    const auto& childLocalCommittedResourceUsage = child->LocalStatistics().CommittedResourceUsage;

    const auto& childResourceUsage = child->ClusterStatistics().ResourceUsage;
    const auto& childCommittedResourceUsage = child->ClusterStatistics().CommittedResourceUsage;

    for (auto* account = this; account; account = account->GetParent()) {
        auto& localStatistics = account->LocalStatistics();
        auto& clusterStatistics = account->ClusterStatistics();

        localStatistics.ResourceUsage -= childLocalResourceUsage;
        clusterStatistics.ResourceUsage -= childResourceUsage;

        localStatistics.CommittedResourceUsage -= childLocalCommittedResourceUsage;
        clusterStatistics.CommittedResourceUsage -= childCommittedResourceUsage;
    }
}

TClusterResources TAccount::ComputeTotalChildrenLimits() const
{
    auto result = TClusterResources();
    for (const auto& [key, child] : KeyToChild()) {
        result += child->ClusterResourceLimits();
    }
    return result;
}

TClusterResources TAccount::ComputeTotalChildrenResourceUsage() const
{
    auto result = TClusterResources();
    for (const auto& [key, child] : KeyToChild()) {
        result += child->ClusterStatistics().ResourceUsage;
    }
    return result;
}

TClusterResources TAccount::ComputeTotalChildrenCommittedResourceUsage() const
{
    auto result = TClusterResources();
    for (const auto& [key, child] : KeyToChild()) {
        result += child->ClusterStatistics().CommittedResourceUsage;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
