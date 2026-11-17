#pragma once

#include "public.h"
#include "acl.h"
#include "cluster_resources.h"
#include "cluster_resource_limits.h"
#include "master_memory_limits.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/map_object.h>
#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/ytlib/object_client/public.h>

#include <yt/yt/core/yson/public.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

const TClusterResourceLimits& GetAccountResourceLimits(
    const TCellGroupToResourceLimits& limits,
    NCellMaster::TMasterCellGroupId groupId);

////////////////////////////////////////////////////////////////////////////////

struct TAccountStatistics
{
    TClusterResources ResourceUsage;
    TClusterResources CommittedResourceUsage;

    void Persist(const NCellMaster::TPersistenceContext& context);

    bool operator==(const TAccountStatistics&) const = default;

    bool IsPersistentlyEmpty() const;

    static const TAccountStatistics Empty;
};

void ToProto(NProto::TAccountStatistics* protoStatistics, const TAccountStatistics& statistics);
void FromProto(TAccountStatistics* statistics, const NProto::TAccountStatistics& protoStatistics);

void Serialize(
    const TAccountStatistics& statistics,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

void FormatValue(TStringBuilderBase* builder, const TAccountStatistics& statistics, TStringBuf /*spec*/);

TAccountStatistics& operator+=(TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics  operator +  (const TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics& operator-=(TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics  operator -  (const TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics  operator -  (const TAccountStatistics& accountStatistics);

////////////////////////////////////////////////////////////////////////////////

// lhs += rhs
void AddToAccountCellStatistics(TCellToStatistics& lhs, const TCellToStatistics& rhs);
// lhs -= rhs
void SubtractFromAccountCellStatistics(TCellToStatistics& lhs, const TCellToStatistics& rhs);

// lhs + rhs
TCellToStatistics AddAccountCellStatistics(const TCellToStatistics& lhs, const TCellToStatistics& rhs);
// lhs - rhs
TCellToStatistics SubtractAccountCellStatistics(const TCellToStatistics& lhs, const TCellToStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Changing this macro changes snapshot serialization format and requires reign promotion.
#define FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX) \
    XX(int, MaxChunkCount, max_chunk_count, EMasterReign()) \
    XX(i64, MaxRowCount, max_row_count, EMasterReign()) \
    XX(i64, MaxDataWeight, max_data_weight, EMasterReign()) \
    XX(i64, MaxUncompressedDataSize, max_uncompressed_data_size, EMasterReign()) \
    XX(i64, MaxCompressedDataSize, max_compressed_data_size, EMasterReign()) \
    XX(i64, MaxInputChunkDataWeight, max_input_chunk_data_weight, EMasterReign()) \
    XX(i64, MaxChunkMetaSize, max_chunk_meta_size, EMasterReign())

struct TChunkMergerCriteria
{
    #define XX(type, camelCaseName, snakeCaseName, reign) std::optional<type> camelCaseName;
    FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX)
    #undef XX

    void AssignNotNull(const TChunkMergerCriteria& rhs);
    bool IsEmpty() const;

    //! Checks that all fields are well-formed, throws otherwise.
    void Validate() const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

void Serialize(const TChunkMergerCriteria& criteria, NYson::IYsonConsumer* consumer);

void Deserialize(TChunkMergerCriteria& criteria, NYTree::INodePtr node);

////////////////////////////////////////////////////////////////////////////////

struct TAccountBackupConfig
{
    TAccountId BackupAccountId;
    NYPath::TYPath BackupPath;
    NYPath::TYPath TrimPathPrefix;

    //! Used only for attribute.
    std::string BackupAccountName;

    void Validate(const NCellMaster::TBootstrap* bootstrap) const;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

void SerializeAccountBackupConfig(
    const TAccountBackupConfig& config,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

TAccountBackupConfig DeserializeAccountBackupConfig(
    NYTree::INodePtr node,
    const NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

class TAccount
    : public NObjectServer::TNonversionedMapObjectBase<TAccount>
{
public:
    DEFINE_BYREF_RW_PROPERTY(TCellToStatistics, CellToStatistics);
    DEFINE_BYREF_RW_PROPERTY(TCellGroupToStatistics, CellGroupToStatistics);
    DEFINE_BYREF_RW_PROPERTY(TCellGroupToResourceLimits, CellGroupToResourceLimits);
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowChildrenLimitOvercommit);
    DEFINE_BYVAL_RW_PROPERTY(int, ChunkMergerNodeTraversalConcurrency, 1);
    DEFINE_BYREF_RW_PROPERTY(TChunkMergerCriteria, ChunkMergerCriteria);
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowUsingChunkMerger, true);

    //! Transient property.
    DEFINE_BYREF_RW_PROPERTY(TDetailedMasterMemory, DetailedMasterMemoryUsage);

    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TAbcConfigPtr, AbcConfig);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<std::string>, FolderId);

    DEFINE_BYREF_RO_PROPERTY(NConcurrency::IReconfigurableThroughputThrottlerPtr, MergeJobThrottler);

    DEFINE_BYVAL_RW_PROPERTY(bool, EnableChunkReincarnation);

    DEFINE_BYVAL_RW_PROPERTY(std::optional<TAccountBackupConfig>, BackupConfig);

    //! Cached |GetAccountShardIndex(id)| for efficient access.
    DEFINE_BYVAL_RO_PROPERTY(i8, ShardIndex);

    //! Cached bucket index to make profiling slightly faster.
    DEFINE_BYVAL_RO_PROPERTY(i8, ProfilingBucketIndex);

public:
    using TNonversionedMapObjectBase<TAccount>::TNonversionedMapObjectBase;
    explicit TAccount(TAccountId id, bool isRoot = false);

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    const TAccountStatistics& GetStatistics() const;
    const TAccountStatistics& GetStatistics(NCellMaster::TMasterCellGroupId groupId) const;

    const TClusterResourceLimits& GetResourceLimits() const;
    const TClusterResourceLimits& GetResourceLimits(NCellMaster::TMasterCellGroupId groupId) const;

    NCellMaster::TMasterCellGroupId GetUniversumGroupId() const;

    void RecomputeGroupStatistics(NCellMaster::TMasterCellGroup* group);

    //! Caches a non-owning pointer to the local cell's entry in CellToStatistics.
    void SetLocalStatisticsPtr(TAccountStatistics *value);

    //! Dereferences the local statistics pointer.
    const TAccountStatistics& LocalStatistics() const;

    //! Applies a usage delta for a cell and updates statistics for all groups containing it.
    void IncreaseCellStatistics(NObjectClient::TCellTag cellTag, const TAccountStatistics& delta);

    //! Replaces statistics for a cell and updates statistics for all groups containing it.
    void SetCellStatistics(NObjectClient::TCellTag cellTag, const TAccountStatistics& statistics);

    //! Returns |true| if disk space limit is exceeded for at least one medium,
    //! i.e. no more disk space could be allocated.
    bool IsDiskSpaceLimitViolated(NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if disk space limit is exceeded for a given medium,
    //! i.e. no more disk space could be allocated.
    bool IsDiskSpaceLimitViolated(int mediumIndex, NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if node count limit is exceeded,
    //! i.e. no more Cypress node could be created.
    bool IsNodeCountLimitViolated(NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if chunk count limit is exceeded,
    //! i.e. no more chunks could be created.
    bool IsChunkCountLimitViolated(NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if tablet count limit is exceeded,
    //! i.e. no more tablets could be created.
    bool IsTabletCountLimitViolated(NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if tablet static memory limit is exceeded,
    //! i.e. no more data could be stored in tablet static memory.
    bool IsTabletStaticMemoryLimitViolated(NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if total master memory usage is exceeded,
    //! i.e. no more master memory can be occupied.
    bool IsTotalMasterMemoryLimitViolated(NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if master memory usage for specified cell is exceeded.
    bool IsCellMasterMemoryLimitViolated(
        NObjectClient::TCellTag cellTag,
        NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns |true| if chunk host master memory usage is exceeded.
    bool IsChunkHostMasterMemoryLimitViolated(NCellMaster::TMasterCellGroupId groupId) const;

    //! Returns total chunk host master memory usage.
    i64 GetChunkHostCellMasterMemoryUsage(NCellMaster::TMasterCellGroupId groupId) const;

    void RecomputeClusterStatistics();

    //! Attaches a child account and adds its resource usage to its new ancestry.
    void AttachChild(const std::string& key, TAccount* child) noexcept override;
    //! Unlinks a child account and subtracts its resource usage from its former ancestry.
    void DetachChild(TAccount* child) noexcept override;

    TClusterResourceLimits ComputeTotalChildrenLimits() const;
    TClusterResourceLimits ComputeTotalChildrenLimits(NCellMaster::TMasterCellGroupId groupId) const;

    TClusterResources ComputeTotalChildrenResourceUsage(
        bool committed,
        NCellMaster::TMasterCellGroupId groupId) const;
    TCellToStatistics ComputeTotalChildrenCellStatistics() const;

    //! Returns violated limits of the account considering its ancestors.
    TViolatedClusterResourceLimits GetViolatedResourceLimits(
        NCellMaster::TBootstrap* bootstrap,
        bool enableTabletResourceValidation,
        NCellMaster::TMasterCellGroupId groupId) const;
    TViolatedClusterResourceLimits GetViolatedResourceLimitsForCell(
        NCellMaster::TBootstrap* bootstrap,
        bool enableTabletResourceValidation,
        NObjectClient::TCellTag cellTag) const;
    //! Returns violated limits of the account considering its successors.
    TViolatedClusterResourceLimits GetRecursiveViolatedResourceLimits(
        NCellMaster::TBootstrap* bootstrap,
        bool enableTabletResourceValidation,
        NCellMaster::TMasterCellGroupId groupId) const;

    int GetMergeJobRateLimit() const;
    void SetMergeJobRateLimit(int mergeJobRateLimit);

    int GetChunkMergerNodeTraversals() const;
    void IncrementChunkMergerNodeTraversals();
    void DecrementChunkMergerNodeTraversals();

private:
    TAccountStatistics* LocalStatisticsPtr_{};

    int MergeJobRateLimit_ = 1;

    //! Indicates the number of nodes currently being processed by the chunk
    //! merger that belong to this account.
    NObjectServer::TEpochRefCounter ChunkMergerNodeTraversals_;

    void AccumulateViolatedResourceLimits(
        TViolatedClusterResourceLimits* violatedLimits,
        NCellMaster::TBootstrap* bootstrap,
        bool enableTabletResourceValidation,
        NCellMaster::TMasterCellGroupId groupId) const;

    std::string GetRootName() const override;
};

DEFINE_MASTER_OBJECT_TYPE(TAccount)

////////////////////////////////////////////////////////////////////////////////

struct TAccountChunkMergerNodeTraversalsPtrContext
    : public NObjectServer::TEphemeralObjectPtrContext
{
    static TAccountChunkMergerNodeTraversalsPtrContext Capture();

    void Ref(NObjectServer::TObject* object);
    void Unref(NObjectServer::TObject* object);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
