#include "account.h"
#include "private.h"
#include "helpers.h"
#include "security_manager.h"

#include <yt/yt/server/master/cell_master/master_cell_group.h>
#include <yt/yt/server/master/cell_master/master_cell_group_manager.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/server/lib/misc/interned_attributes.h>

#include <yt/yt/server/lib/security_server/proto/security_manager.pb.h>

#include <yt/yt/ytlib/object_client/config.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ypath/tokenizer.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NSecurityServer {

using namespace NYson;
using namespace NYTree;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NConcurrency;
using namespace NServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = SecurityServerLogger;
constinit const auto Profiler = AccountProfiler;

////////////////////////////////////////////////////////////////////////////////

//! Missing group limits impose no additional restriction on an account.
const TClusterResourceLimits& GetAccountResourceLimits(
    const TCellGroupToResourceLimits& limits,
    TMasterCellGroupId groupId)
{
    static const auto Infinity = TClusterResourceLimits::Infinity();
    auto it = limits.find(groupId);
    return it == limits.end() ? Infinity : it->second;
}

////////////////////////////////////////////////////////////////////////////////

void TAccountStatistics::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, ResourceUsage);
    Persist(context, CommittedResourceUsage);
}

bool TAccountStatistics::IsPersistentlyEmpty() const
{
    return ResourceUsage.IsPersistentlyEmpty() && CommittedResourceUsage.IsPersistentlyEmpty();
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

void Serialize(const TAccountStatistics& statistics, IYsonConsumer* consumer, const TBootstrap* bootstrap)
{
    auto fluent = BuildYsonFluently(consumer)
        .BeginMap();

    fluent
        .Item("resource_usage");
    SerializeClusterResources(statistics.ResourceUsage, fluent.GetConsumer(), bootstrap);

    fluent
        .Item("committed_resource_usage");
    SerializeClusterResources(statistics.CommittedResourceUsage, fluent.GetConsumer(), bootstrap);

    fluent
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TAccountStatistics& statistics, TStringBuf /*spec*/)
{
    builder->AppendFormat("{ResourceUsage: %v, CommittedResourceUsage: %v}",
        statistics.ResourceUsage,
        statistics.CommittedResourceUsage);
}

const TAccountStatistics TAccountStatistics::Empty = TAccountStatistics();

////////////////////////////////////////////////////////////////////////////////

TAccountStatistics& operator+=(TAccountStatistics& lhs, const TAccountStatistics& rhs)
{
    lhs.ResourceUsage += rhs.ResourceUsage;
    lhs.CommittedResourceUsage += rhs.CommittedResourceUsage;
    return lhs;
}

TAccountStatistics operator+(const TAccountStatistics& lhs, const TAccountStatistics& rhs)
{
    auto result = lhs;
    result += rhs;
    return result;
}

TAccountStatistics& operator-=(TAccountStatistics& lhs, const TAccountStatistics& rhs)
{
    lhs.ResourceUsage -= rhs.ResourceUsage;
    lhs.CommittedResourceUsage -= rhs.CommittedResourceUsage;
    return lhs;
}

TAccountStatistics operator-(const TAccountStatistics& lhs, const TAccountStatistics& rhs)
{
    auto result = lhs;
    result -= rhs;
    return result;
}

TAccountStatistics operator-(const TAccountStatistics& accountStatistics)
{
    return TAccountStatistics(-accountStatistics.ResourceUsage, -accountStatistics.CommittedResourceUsage);
}

////////////////////////////////////////////////////////////////////////////////

void AddToAccountCellStatistics(
    TCellToStatistics& lhs,
    const TCellToStatistics& rhs)
{
    for (const auto& [cellTag, accountStatistics] : rhs) {
        lhs[cellTag] += accountStatistics;
    }
}

void SubtractFromAccountCellStatistics(
    TCellToStatistics& lhs,
    const TCellToStatistics& rhs)
{
    for (const auto& [cellTag, accountStatistics] : rhs) {
        lhs[cellTag] -= accountStatistics;
    }
}

TCellToStatistics AddAccountCellStatistics(
    const TCellToStatistics& lhs,
    const TCellToStatistics& rhs)
{
    auto result = lhs;
    AddToAccountCellStatistics(result, rhs);
    return result;
}

TCellToStatistics SubtractAccountCellStatistics(
    const TCellToStatistics& lhs,
    const TCellToStatistics& rhs)
{
    auto result = lhs;
    SubtractFromAccountCellStatistics(result, rhs);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

void TChunkMergerCriteria::AssignNotNull(const TChunkMergerCriteria& rhs) {
#define XX(type, camelCaseName, snakeCaseName, reign) \
    if (rhs.camelCaseName) { \
        camelCaseName = rhs.camelCaseName; \
    }

    FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX)
#undef XX
}

bool TChunkMergerCriteria::IsEmpty() const {
#define XX(type, camelCaseName, snakeCaseName, reign) camelCaseName ||
    return !(FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX) false);
#undef XX
}

void TChunkMergerCriteria::Validate() const {
    if (MaxChunkCount && MaxChunkCount <= 1) {
        THROW_ERROR_EXCEPTION("Invalid chunk merger criteria: expected max_chunk_count > 1, found %v",
            *MaxChunkCount);
    }

#define XX(type, camelCaseName, snakeCaseName, reign) \
    if (camelCaseName && camelCaseName <= 0) { \
        THROW_ERROR_EXCEPTION("Invalid chunk merger criteria: " #snakeCaseName " must be positive, found %v", \
            *camelCaseName); \
    }

    FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX)
#undef XX
}

void TChunkMergerCriteria::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
#define XX(type, camelCaseName, snakeCaseName, reign) Save(context, camelCaseName);
    FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX)
#undef XX
}

void TChunkMergerCriteria::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
#define XX(type, camelCaseName, snakeCaseName, reign) \
    if (context.GetVersion() >= reign) { \
        Load(context, camelCaseName); \
    }

    FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX)
#undef XX
}

void Serialize(const TChunkMergerCriteria& criteria, NYson::IYsonConsumer* consumer)
{
#define XX(type, camelCaseName, snakeCaseName, reign) \
    .DoIf(criteria.camelCaseName.has_value(), [&] (TFluentMap fluent) { \
        fluent.Item(#snakeCaseName).Value(criteria.camelCaseName); \
    })

    BuildYsonFluently(consumer)
        .BeginMap()
            FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX)
        .EndMap();
#undef XX
}

void Deserialize(TChunkMergerCriteria& criteria, NYTree::INodePtr node)
{
    TChunkMergerCriteria result;

    auto map = node->AsMap();

#define XX(type, camelCaseName, snakeCaseName, reign) \
    result.camelCaseName = map->FindChildValue<type>(#snakeCaseName);

    FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX)
#undef XX

    criteria = std::move(result);
}

////////////////////////////////////////////////////////////////////////////////

void ValidateCorrectPath(const TYPath& path)
{
    NYPath::TTokenizer tokenizer(path);
    tokenizer.Advance();
    tokenizer.Expect(NYPath::ETokenType::Slash);
    tokenizer.Advance();
    while (tokenizer.GetType() != NYPath::ETokenType::EndOfStream) {
        tokenizer.Expect(NYPath::ETokenType::Slash);
        tokenizer.Advance();
        tokenizer.Expect(NYPath::ETokenType::Literal);
        tokenizer.Advance();
    }
}

void TAccountBackupConfig::Validate(const TBootstrap* bootstrap) const
{
    const auto& securityManager = bootstrap->GetSecurityManager();

    auto* backupAccount = securityManager->FindAccount(BackupAccountId);
    if (!backupAccount) {
        THROW_ERROR_EXCEPTION("Invalid backup config: backup account %v not found", BackupAccountId);
    }
    if (!IsObjectActive(backupAccount)) {
        THROW_ERROR_EXCEPTION("Invalid backup config: backup account %v is removed", BackupAccountId);
    }

    ValidateCorrectPath(BackupPath);
    if (!TrimPathPrefix.empty()) {
        ValidateCorrectPath(TrimPathPrefix);
    }
}

void TAccountBackupConfig::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, BackupAccountId);
    Save(context, BackupPath);
    Save(context, TrimPathPrefix);
}

void TAccountBackupConfig::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, BackupAccountId);
    Load(context, BackupPath);
    Load(context, TrimPathPrefix);
}

void SerializeAccountBackupConfig(
    const TAccountBackupConfig& config,
    NYson::IYsonConsumer* consumer,
    const TBootstrap* bootstrap)
{
    try {
        config.Validate(bootstrap);
    } catch (const std::exception& e) {
        YT_TLOG_ALERT_AND_THROW("Failed to validate account during backup config serialization")
            .With(e);
    }

    const auto& securityManager = bootstrap->GetSecurityManager();
    const auto* backupAccount = securityManager->GetAccount(config.BackupAccountId);

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("backup_account").Value(backupAccount->GetName())
            .Item("backup_path").Value(config.BackupPath)
            .Item("trim_path_prefix").Value(config.TrimPathPrefix)
        .EndMap();
}

TAccountBackupConfig DeserializeAccountBackupConfig(
    INodePtr node,
    const TBootstrap* bootstrap)
{
    const auto& securityManager = bootstrap->GetSecurityManager();

    auto map = node->AsMap();
    auto backupAccountName = map->FindChild("backup_account")->AsString()->GetValue();

    const auto* backupAccount = securityManager->FindAccountByName(backupAccountName, /*activeLifeStageOnly*/ true);
    if (!backupAccount) {
        THROW_ERROR_EXCEPTION("Invalid backup config: backup account %Qv not found", backupAccountName);
    }
    TAccountBackupConfig config;

    config.BackupAccountId = backupAccount->GetId();
    config.BackupPath = map->FindChild("backup_path")->AsString()->GetValue();
    config.TrimPathPrefix = map->FindChild("trim_path_prefix")->AsString()->GetValue();
    return config;
}

////////////////////////////////////////////////////////////////////////////////

TAccount::TAccount(TAccountId id, bool isRoot)
    : TNonversionedMapObjectBase<TAccount>(id, isRoot)
    , MergeJobThrottler_(CreateReconfigurableThroughputThrottler(
        TThroughputThrottlerConfig::Create(0),
        Logger(),
        Profiler().WithPrefix("/merge_job_throttler")))
    , ShardIndex_(GetAccountShardIndex(id))
    , ProfilingBucketIndex_(GetAccountProfilingBucketIndex(id))
    , ChunkMergerNodeTraversals_(id)
{
    CellGroupToResourceLimits_.emplace(
        GetUniversumGroupId(),
        TClusterResourceLimits::DefaultUniversumLimits());
    CellGroupToStatistics_.emplace(GetUniversumGroupId(), TAccountStatistics());
    MergeJobThrottler_->SetLimit(MergeJobRateLimit_);
}

std::string TAccount::GetLowercaseObjectName() const
{
    return Format("account %Qv", GetName());
}

std::string TAccount::GetCapitalizedObjectName() const
{
    return Format("Account %Qv", GetName());
}

TYPath TAccount::GetObjectPath() const
{
    return Format("//sys/accounts/%v", NYPath::ToYPathLiteral(GetName()));
}

std::string TAccount::GetRootName() const
{
    YT_VERIFY(IsRoot());
    return NSecurityClient::RootAccountName;
}

void TAccount::Save(NCellMaster::TSaveContext& context) const
{
    TNonversionedMapObjectBase<TAccount>::Save(context);

    using NYT::Save;
    Save(context, CellGroupToStatistics_);
    Save(context, CellToStatistics_);
    Save(context, CellGroupToResourceLimits_);
    Save(context, AllowChildrenLimitOvercommit_);
    Save(context, MergeJobRateLimit_);
    TNullableIntrusivePtrSerializer<>::Save(context, AbcConfig_);
    Save(context, FolderId_);
    Save(context, ChunkMergerNodeTraversalConcurrency_);
    Save(context, AllowUsingChunkMerger_);
    Save(context, ChunkMergerCriteria_);
    Save(context, EnableChunkReincarnation_);
    Save(context, BackupConfig_);
}

void TAccount::Load(NCellMaster::TLoadContext& context)
{
    TNonversionedMapObjectBase<TAccount>::Load(context);

    using NYT::Load;
    if (context.GetVersion() >= EMasterReign::AccountMasterCellGroupLimits) {
        Load(context, CellGroupToStatistics_);
        Load(context, CellToStatistics_);
        Load(context, CellGroupToResourceLimits_);
    } else {
        Load(context, CellGroupToStatistics_[GetUniversumGroupId()]);
        Load(context, CellToStatistics_);
        Load(context, GetOrCrash(CellGroupToResourceLimits_, GetUniversumGroupId()));
    }
    Load(context, AllowChildrenLimitOvercommit_);
    Load(context, MergeJobRateLimit_);
    TNullableIntrusivePtrSerializer<>::Load(context, AbcConfig_);
    Load(context, FolderId_);
    Load(context, ChunkMergerNodeTraversalConcurrency_);

    // COMPAT(aleksandra-zh)
    const auto& attributeName = EInternedAttributeKey::ChunkMergerNodeTraversalConcurrency.Unintern();
    if (auto value = FindAttribute(attributeName)) {
        YT_VERIFY(Attributes_->TryRemove(attributeName));
        YT_TLOG_INFO("Dropping custom chunk merger traversal concurrency value")
            .With("Value", *value)
            .With("AccountId", GetId());
    }

    Load(context, AllowUsingChunkMerger_);
    Load(context, ChunkMergerCriteria_);
    Load(context, EnableChunkReincarnation_);

    // COMPAT(grphil)
    if ((context.GetVersion() >= EMasterReign::TableBackupAttributes && context.GetVersion() < EMasterReign::Start_26_2) ||
        context.GetVersion() >= EMasterReign::TableBackupAttributes_26_2)
    {
        Load(context, BackupConfig_);
    }

    MergeJobThrottler_->SetLimit(MergeJobRateLimit_);
}

TMasterCellGroupId TAccount::GetUniversumGroupId() const
{
    return NCellMaster::GetUniversumGroupId(CellTagFromId(GetId()));
}

const TAccountStatistics& TAccount::GetStatistics() const
{
    return GetStatistics(GetUniversumGroupId());
}

const TAccountStatistics& TAccount::GetStatistics(TMasterCellGroupId groupId) const
{
    auto it = CellGroupToStatistics_.find(groupId);
    return it == CellGroupToStatistics_.end() ? TAccountStatistics::Empty : it->second;
}

const TClusterResourceLimits& TAccount::GetResourceLimits() const
{
    return GetResourceLimits(GetUniversumGroupId());
}

const TClusterResourceLimits& TAccount::GetResourceLimits(TMasterCellGroupId groupId) const
{
    return GetAccountResourceLimits(CellGroupToResourceLimits_, groupId);
}

void TAccount::RecomputeGroupStatistics(TMasterCellGroup* group)
{
    auto& result = CellGroupToStatistics_[group->GetId()];
    result = {};
    for (const auto& [cellTag, statistics] : CellToStatistics_) {
        if (group->Contains(cellTag)) {
            result += statistics;
        }
    }
}

void TAccount::SetLocalStatisticsPtr(TAccountStatistics *value)
{
    LocalStatisticsPtr_ = value;
}

const TAccountStatistics& TAccount::LocalStatistics() const
{
    return *LocalStatisticsPtr_;
}

void TAccount::IncreaseCellStatistics(TCellTag cellTag, const TAccountStatistics& delta)
{
    GetOrCrash(CellToStatistics_, cellTag) += delta;

    const auto& groupManager = GetBootstrap()->GetMasterCellGroupManager();
    for (auto* group : groupManager->GetMasterCellGroupsForCell(cellTag)) {
        CellGroupToStatistics_[group->GetId()] += delta;
    }
}

void TAccount::SetCellStatistics(TCellTag cellTag, const TAccountStatistics& statistics)
{
    auto& cellStatistics = GetOrCrash(CellToStatistics_, cellTag);
    auto delta = statistics - cellStatistics;
    cellStatistics = statistics;

    const auto& groupManager = GetBootstrap()->GetMasterCellGroupManager();
    for (auto* group : groupManager->GetMasterCellGroupsForCell(cellTag)) {
        CellGroupToStatistics_[group->GetId()] += delta;
    }
}

bool TAccount::IsDiskSpaceLimitViolated(TMasterCellGroupId groupId) const
{
    for (auto [mediumIndex, diskSpace] : GetStatistics(groupId).ResourceUsage.DiskSpace()) {
        if (TLimit64(diskSpace) > GetResourceLimits(groupId).DiskSpace().GetOrDefault(mediumIndex)) {
            return true;
        }
    }
    return false;
}

bool TAccount::IsDiskSpaceLimitViolated(int mediumIndex, TMasterCellGroupId groupId) const
{
    auto usageSpace = GetOrDefault(GetStatistics(groupId).ResourceUsage.DiskSpace(), mediumIndex);
    auto limitsSpace = GetResourceLimits(groupId).DiskSpace().GetOrDefault(mediumIndex);
    return TLimit64(usageSpace) > limitsSpace;
}

bool TAccount::IsNodeCountLimitViolated(TMasterCellGroupId groupId) const
{
    // See TSecurityManager::ValidateResourceUsageIncrease for the reason why committed usage is compared here.
    return TLimit64(GetStatistics(groupId).CommittedResourceUsage.GetNodeCount()) >
        GetResourceLimits(groupId).GetNodeCount();
}

bool TAccount::IsChunkCountLimitViolated(TMasterCellGroupId groupId) const
{
    return TLimit64(GetStatistics(groupId).ResourceUsage.GetChunkCount()) >
        GetResourceLimits(groupId).GetChunkCount();
}

bool TAccount::IsTabletCountLimitViolated(TMasterCellGroupId groupId) const
{
    return TLimit32(GetStatistics(groupId).ResourceUsage.GetTabletCount()) >
        GetResourceLimits(groupId).GetTabletCount();
}

bool TAccount::IsTabletStaticMemoryLimitViolated(TMasterCellGroupId groupId) const
{
    return TLimit64(GetStatistics(groupId).ResourceUsage.GetTabletStaticMemory()) >
        GetResourceLimits(groupId).GetTabletStaticMemory();
}

bool TAccount::IsTotalMasterMemoryLimitViolated(TMasterCellGroupId groupId) const
{
    return TLimit64(GetStatistics(groupId).ResourceUsage.GetTotalMasterMemory()) >
        GetResourceLimits(groupId).MasterMemory().Total;
}

bool TAccount::IsCellMasterMemoryLimitViolated(TCellTag cellTag, TMasterCellGroupId groupId) const
{
    if (groupId != GetUniversumGroupId()) {
        const auto& groupManager = GetBootstrap()->GetMasterCellGroupManager();
        auto* group = groupManager->FindMasterCellGroup(groupId);
        if (!IsObjectAlive(group) || !group->Contains(cellTag)) {
            return false;
        }
    }
    const auto& perCellLimits = GetResourceLimits(groupId).MasterMemory().PerCell;
    auto limitsIt = perCellLimits.find(cellTag);
    if (limitsIt == perCellLimits.end()) {
        return false;
    }

    auto usageIt = CellToStatistics_.find(cellTag);
    if (usageIt == CellToStatistics_.end()) {
        return false;
    }

    return TLimit64(usageIt->second.ResourceUsage.GetTotalMasterMemory()) > limitsIt->second;
}

bool TAccount::IsChunkHostMasterMemoryLimitViolated(TMasterCellGroupId groupId) const
{
    auto totalChunkHostMasterMemory = GetChunkHostCellMasterMemoryUsage(groupId);
    return TLimit64(totalChunkHostMasterMemory) > GetResourceLimits(groupId).MasterMemory().ChunkHost;
}

i64 TAccount::GetChunkHostCellMasterMemoryUsage(TMasterCellGroupId groupId) const
{
    return GetStatistics(groupId).ResourceUsage.GetChunkHostCellMasterMemory();
}

void TAccount::RecomputeClusterStatistics()
{
    CellGroupToStatistics_.clear();
    const auto& groupManager = GetBootstrap()->GetMasterCellGroupManager();
    for (auto [groupId, group] : groupManager->MasterCellGroups()) {
        if (IsObjectAlive(group)) {
            RecomputeGroupStatistics(group);
        }
    }
}

void TAccount::AttachChild(const std::string& key, TAccount* child) noexcept
{
    TNonversionedMapObjectBase<TAccount>::AttachChild(key, child);

    const auto& childLocalStatistics = child->LocalStatistics();
    const auto& childMasterMemoryUsage = child->DetailedMasterMemoryUsage();

    const auto& securityManager = GetBootstrap()->GetSecurityManager();

    for (auto* account = this; account; account = account->GetParent()) {
        auto& masterMemoryUsage = account->DetailedMasterMemoryUsage();

        securityManager->IncreaseLocalAndClusterAccountStatistics(account, childLocalStatistics);

        masterMemoryUsage += childMasterMemoryUsage;
    }
}

void TAccount::DetachChild(TAccount* child) noexcept
{
    TNonversionedMapObjectBase<TAccount>::DetachChild(child);

    const auto& childLocalStatistics = child->LocalStatistics();
    const auto& childMasterMemoryUsage = child->DetailedMasterMemoryUsage();

    const auto& securityManager = GetBootstrap()->GetSecurityManager();

    for (auto* account = this; account; account = account->GetParent()) {
        auto& masterMemoryUsage = account->DetailedMasterMemoryUsage();

        securityManager->IncreaseLocalAndClusterAccountStatistics(account, -childLocalStatistics);

        masterMemoryUsage -= childMasterMemoryUsage;
    }
}

TClusterResourceLimits TAccount::ComputeTotalChildrenLimits() const
{
    return ComputeTotalChildrenLimits(GetUniversumGroupId());
}

TClusterResourceLimits TAccount::ComputeTotalChildrenLimits(TMasterCellGroupId groupId) const
{
    auto result = TClusterResourceLimits::Zero();
    for (const auto& [key, child] : KeyToChild()) {
        const auto& childLimits = child->CellGroupToResourceLimits();
        if (auto it = childLimits.find(groupId); it != childLimits.end()) {
            result.IncreaseWithInfinityAllowed(it->second);
        }
    }
    return result;
}

TClusterResources TAccount::ComputeTotalChildrenResourceUsage(bool committed, TMasterCellGroupId groupId) const
{
    auto result = TClusterResources();
    for (const auto& [key, child] : KeyToChild()) {
        result += committed
            ? child->GetStatistics(groupId).CommittedResourceUsage
            : child->GetStatistics(groupId).ResourceUsage;
    }
    return result;
}

TCellToStatistics TAccount::ComputeTotalChildrenCellStatistics() const
{
    auto result = TCellToStatistics();
    for (const auto& [key, child] : KeyToChild()) {
        AddToAccountCellStatistics(result, child->CellToStatistics());
    }
    return result;
}

TViolatedClusterResourceLimits TAccount::GetViolatedResourceLimits(
    NCellMaster::TBootstrap* bootstrap,
    bool enableTabletResourceValidation,
    TMasterCellGroupId groupId) const
{
    TViolatedClusterResourceLimits violatedLimits;
    AccumulateViolatedResourceLimits(&violatedLimits, bootstrap, enableTabletResourceValidation, groupId);
    return violatedLimits;
}

TViolatedClusterResourceLimits TAccount::GetViolatedResourceLimitsForCell(
    NCellMaster::TBootstrap* bootstrap,
    bool enableTabletResourceValidation,
    TCellTag cellTag) const
{
    TViolatedClusterResourceLimits violatedLimits;
    const auto& groupManager = bootstrap->GetMasterCellGroupManager();
    for (auto* group : groupManager->GetMasterCellGroupsForCell(cellTag)) {
        AccumulateViolatedResourceLimits(&violatedLimits, bootstrap, enableTabletResourceValidation, group->GetId());
    }
    return violatedLimits;
}

void TAccount::AccumulateViolatedResourceLimits(
    TViolatedClusterResourceLimits* violatedLimits,
    NCellMaster::TBootstrap* bootstrap,
    bool enableTabletResourceValidation,
    TMasterCellGroupId groupId) const
{
    const auto& multicellManager = bootstrap->GetMulticellManager();

    auto selfCellTag = multicellManager->GetCellTag();
    const auto& cellTags = multicellManager->GetRegisteredMasterCellTags();

    for (auto* account = this; account; account = account->GetParent()) {
        if (account->IsNodeCountLimitViolated(groupId)) {
            violatedLimits->SetNodeCount(1);
        }
        if (account->IsChunkCountLimitViolated(groupId)) {
            violatedLimits->SetChunkCount(1);
        }

        if (enableTabletResourceValidation) {
            if (account->IsTabletCountLimitViolated(groupId)) {
                violatedLimits->SetTabletCount(1);
            }
            if (account->IsTabletStaticMemoryLimitViolated(groupId)) {
                violatedLimits->SetTabletStaticMemory(1);
            }
        }

        for (const auto& [mediumIndex, usage] : account->GetStatistics(groupId).ResourceUsage.DiskSpace()) {
            if (account->IsDiskSpaceLimitViolated(mediumIndex, groupId)) {
                violatedLimits->SetMediumDiskSpace(mediumIndex, 1);
            } else {
                violatedLimits->AddToMediumDiskSpace(mediumIndex, 0);
            }
        }
        for (const auto& [mediumIndex, usage] : account->GetResourceLimits(groupId).DiskSpace()) {
            violatedLimits->AddToMediumDiskSpace(mediumIndex, 0);
        }

        if (account->IsTotalMasterMemoryLimitViolated(groupId)) {
            violatedLimits->MasterMemory().Total = 1;
        }
        if (account->IsChunkHostMasterMemoryLimitViolated(groupId)) {
            violatedLimits->MasterMemory().ChunkHost = 1;
        }
        if (account->IsCellMasterMemoryLimitViolated(selfCellTag, groupId)) {
            violatedLimits->MasterMemory().PerCell[selfCellTag] = 1;
        }
        for (auto cellTag : cellTags) {
            if (account->IsCellMasterMemoryLimitViolated(cellTag, groupId)) {
                violatedLimits->MasterMemory().PerCell[cellTag] = 1;
            }
        }
    }
}

TViolatedClusterResourceLimits TAccount::GetRecursiveViolatedResourceLimits(
    NCellMaster::TBootstrap* bootstrap,
    bool enableTabletResourceValidation,
    TMasterCellGroupId groupId) const
{
    const auto& multicellManager = bootstrap->GetMulticellManager();

    auto selfCellTag = multicellManager->GetCellTag();
    const auto& cellTags = multicellManager->GetRegisteredMasterCellTags();

    return AccumulateOverMapObjectSubtree(
        this,
        TViolatedClusterResourceLimits(),
        [&] (const TAccount* account, TViolatedClusterResourceLimits* violatedLimits) {
            if (account->IsNodeCountLimitViolated(groupId)) {
                violatedLimits->SetNodeCount(violatedLimits->GetNodeCount() + 1);
            }
            if (account->IsChunkCountLimitViolated(groupId)) {
                violatedLimits->SetChunkCount(violatedLimits->GetChunkCount() + 1);
            }

            if (enableTabletResourceValidation) {
                if (account->IsTabletCountLimitViolated(groupId)) {
                    violatedLimits->SetTabletCount(violatedLimits->GetTabletCount() + 1);
                }
                if (account->IsTabletStaticMemoryLimitViolated(groupId)) {
                    violatedLimits->SetTabletStaticMemory(violatedLimits->GetTabletStaticMemory() + 1);
                }
            }

            for (const auto& [mediumIndex, usage] : account->GetStatistics(groupId).ResourceUsage.DiskSpace()) {
                if (account->IsDiskSpaceLimitViolated(mediumIndex, groupId)) {
                    violatedLimits->AddToMediumDiskSpace(mediumIndex, 1);
                } else {
                    violatedLimits->AddToMediumDiskSpace(mediumIndex, 0);
                }
            }
            for (const auto& [mediumIndex, usage] : account->GetResourceLimits(groupId).DiskSpace()) {
                violatedLimits->AddToMediumDiskSpace(mediumIndex, 0);
            }

            if (account->IsTotalMasterMemoryLimitViolated(groupId)) {
                ++violatedLimits->MasterMemory().Total;
            }
            if (account->IsChunkHostMasterMemoryLimitViolated(groupId)) {
                ++violatedLimits->MasterMemory().ChunkHost;
            }
            if (account->IsCellMasterMemoryLimitViolated(selfCellTag, groupId)) {
                ++violatedLimits->MasterMemory().PerCell[selfCellTag];
            }
            for (auto cellTag : cellTags) {
                if (account->IsCellMasterMemoryLimitViolated(cellTag, groupId)) {
                    ++violatedLimits->MasterMemory().PerCell[cellTag];
                }
            }
        });
}

int TAccount::GetMergeJobRateLimit() const
{
    return MergeJobRateLimit_;
}

void TAccount::SetMergeJobRateLimit(int mergeJobRateLimit)
{
    MergeJobRateLimit_ = mergeJobRateLimit;
    MergeJobThrottler_->SetLimit(MergeJobRateLimit_);
}

int TAccount::GetChunkMergerNodeTraversals() const
{
    return ChunkMergerNodeTraversals_.GetValue();
}

void TAccount::IncrementChunkMergerNodeTraversals()
{
    ChunkMergerNodeTraversals_.Increment(+1);
}

void TAccount::DecrementChunkMergerNodeTraversals()
{
    ChunkMergerNodeTraversals_.Increment(-1);
}

////////////////////////////////////////////////////////////////////////////////

/*static*/ TAccountChunkMergerNodeTraversalsPtrContext TAccountChunkMergerNodeTraversalsPtrContext::Capture()
{
    TAccountChunkMergerNodeTraversalsPtrContext result;
    static_cast<TEphemeralObjectPtrContext&>(result) = TEphemeralObjectPtrContext::Capture();
    return result;
}

void TAccountChunkMergerNodeTraversalsPtrContext::Ref(TObject* object)
{
    auto* account = object->As<TAccount>();
    account->IncrementChunkMergerNodeTraversals();

    TEphemeralObjectPtrContext::Ref(object);
}

void TAccountChunkMergerNodeTraversalsPtrContext::Unref(TObject* object)
{
    SafeUnref([account = object->As<TAccount>()] {
        account->DecrementChunkMergerNodeTraversals();
    });

    TEphemeralObjectPtrContext::Unref(object);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
