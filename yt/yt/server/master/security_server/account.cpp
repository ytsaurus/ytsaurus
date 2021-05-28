#include "account.h"
#include "private.h"

#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectServer;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = SecurityServerLogger;

////////////////////////////////////////////////////////////////////////////////

void TAccountStatistics::Persist(const NCellMaster::TPersistenceContext& context)
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
    Save(context, MergeJobRateLimit_);
    Save(context, AbcConfig_.operator bool());
    if (AbcConfig_) {
        Save(context, *AbcConfig_);
    }
    Save(context, FolderId_);
}

void TAccount::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedMapObjectBase<TAccount>::Load(context);

    using NYT::Load;
    Load(context, ClusterStatistics_);
    Load(context, MulticellStatistics_);
    Load(context, ClusterResourceLimits_);
    Load(context, AllowChildrenLimitOvercommit_);

    // COMPAT(aleksandra-zh)
    if (context.GetVersion() >= EMasterReign::MasterMergeJobs) {
        Load(context, MergeJobRateLimit_);
    }

    // COMPAT(cookiedoth)
    if (context.GetVersion() < EMasterReign::MakeAbcFolderIdBuiltin) {
        auto moveUserToBuiltinAttribute = [&] (auto& field, TInternedAttributeKey internedAttributeKey) {
            const auto& attributeName = internedAttributeKey.Unintern();
            if (auto attribute = FindAttribute(attributeName)) {
                auto value = std::move(*attribute);
                YT_VERIFY(Attributes_->Remove(attributeName));
                try {
                    field = ConvertTo<std::decay_t<decltype(field)>>(value);
                } catch (const std::exception& ex) {
                    YT_LOG_WARNING(ex, "Cannot parse %Qv attribute (Value: %v, AccountId: %v)",
                        attributeName,
                        value,
                        GetId());
                }
            }
        };
        moveUserToBuiltinAttribute(AbcConfig_, EInternedAttributeKey::Abc);
        moveUserToBuiltinAttribute(FolderId_, EInternedAttributeKey::FolderId);
    } else {
        if (Load<bool>(context)) {
            AbcConfig_ = New<NObjectClient::TAbcConfig>();
            Load(context, *AbcConfig_);
        }
        Load(context, FolderId_);
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
    return ClusterStatistics_.CommittedResourceUsage.GetNodeCount() > ClusterResourceLimits_.GetNodeCount();
}

bool TAccount::IsChunkCountLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.GetChunkCount() > ClusterResourceLimits_.GetChunkCount();
}

bool TAccount::IsTabletCountLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.GetTabletCount() > ClusterResourceLimits_.GetTabletCount();
}

bool TAccount::IsTabletStaticMemoryLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.GetTabletStaticMemory() > ClusterResourceLimits_.GetTabletStaticMemory();
}

bool TAccount::IsMasterMemoryLimitViolated() const
{
    return ClusterStatistics_.ResourceUsage.GetTotalMasterMemory() > ClusterResourceLimits_.GetMasterMemory();
}

bool TAccount::IsMasterMemoryLimitViolated(TCellTag cellTag) const
{
    const auto& perCellLimits = ClusterResourceLimits_.CellMasterMemoryLimits();
    auto limitIt = perCellLimits.find(cellTag);
    if (limitIt == perCellLimits.end()) {
        return false;
    }

    auto usageIt = MulticellStatistics_.find(cellTag);
    if (usageIt == MulticellStatistics_.end()) {
        return false;
    }

    return usageIt->second.ResourceUsage.GetTotalMasterMemory() > limitIt->second;
}

bool TAccount::IsChunkHostMasterMemoryLimitViolated(const TMulticellManagerPtr& multicellManager) const
{
    auto totalRoleMasterMemory = GetChunkHostMasterMemoryUsage(multicellManager);
    return totalRoleMasterMemory > ClusterResourceLimits_.GetChunkHostMasterMemory();
}

i64 TAccount::GetChunkHostMasterMemoryUsage(const TMulticellManagerPtr& multicellManager) const
{
    auto cellTags = multicellManager->GetRoleMasterCells(EMasterCellRole::ChunkHost);
    i64 totalRoleMasterMemory = 0;
    for (auto cellTag : cellTags) {
        auto roleMulticellStatisticsIt = MulticellStatistics_.find(cellTag);
        if (roleMulticellStatisticsIt != MulticellStatistics_.end()) {
            totalRoleMasterMemory += roleMulticellStatisticsIt->second.ResourceUsage.GetTotalMasterMemory();
        }
    }

    return totalRoleMasterMemory;
}

TAccountStatistics* TAccount::GetCellStatistics(NObjectClient::TCellTag cellTag)
{
    return &GetOrCrash(MulticellStatistics_, cellTag);
}

void TAccount::RecomputeClusterStatistics()
{
    ClusterStatistics_ = TAccountStatistics();
    for (const auto& [cellTag, statistics] : MulticellStatistics_) {
        ClusterStatistics_ += statistics;
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

TClusterResourceLimits TAccount::ComputeTotalChildrenLimits() const
{
    auto result = TClusterResourceLimits();
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

int TAccount::GetMergeJobRateLimit() const
{
    return MergeJobRateLimit_;
}

void TAccount::SetMergeJobRateLimit(int mergeJobRateLimit)
{
    MergeJobRateLimit_ = mergeJobRateLimit;
}

int TAccount::GetMergeJobRate() const
{
    return MergeJobRate_;
}

void TAccount::IncrementMergeJobRate(int value)
{
    MergeJobRate_ += value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
