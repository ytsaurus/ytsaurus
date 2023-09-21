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

#include <yt/yt/core/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TAccountStatistics
{
    TClusterResources ResourceUsage;
    TClusterResources CommittedResourceUsage;

    void Persist(const NCellMaster::TPersistenceContext& context);
};

void ToProto(NProto::TAccountStatistics* protoStatistics, const TAccountStatistics& statistics);
void FromProto(TAccountStatistics* statistics, const NProto::TAccountStatistics& protoStatistics);

void Serialize(
    const TAccountStatistics& statistics,
    NYson::IYsonConsumer* consumer,
    const NCellMaster::TBootstrap* bootstrap);

TAccountStatistics& operator += (TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics  operator +  (const TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics& operator -= (TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics  operator -  (const TAccountStatistics& lhs, const TAccountStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

// lhs += rhs
void AddToAccountMulticellStatistics(TAccountMulticellStatistics& lhs, const TAccountMulticellStatistics& rhs);
// lhs -= rhs
void SubtractFromAccountMulticellStatistics(TAccountMulticellStatistics& lhs, const TAccountMulticellStatistics& rhs);

// lhs + rhs
TAccountMulticellStatistics AddAccountMulticellStatistics(const TAccountMulticellStatistics& lhs, const TAccountMulticellStatistics& rhs);
// lhs - rhs
TAccountMulticellStatistics SubtractAccountMulticellStatistics(const TAccountMulticellStatistics& lhs, const TAccountMulticellStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

//! Changing this macro changes snapshot serialization format and requires reign promotion.
#define FOR_EACH_CHUNK_MERGER_CRITERIA_FIELD(XX) \
    XX(int, MaxChunkCount, max_chunk_count) \
    XX(i64, MaxRowCount, max_row_count) \
    XX(i64, MaxDataWeight, max_data_weight) \
    XX(i64, MaxUncompressedDataSize, max_uncompressed_data_size) \
    XX(i64, MaxCompressedDataSize, max_compressed_data_size) \
    XX(i64, MaxInputChunkDataWeight, max_input_chunk_data_weight)

struct TChunkMergerCriteria
{
    #define XX(type, camelCaseName, snakeCaseName) std::optional<type> camelCaseName;
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

class TAccount
    : public NObjectServer::TNonversionedMapObjectBase<TAccount>
{
public:
    DEFINE_BYREF_RW_PROPERTY(TAccountMulticellStatistics, MulticellStatistics);
    DEFINE_BYVAL_RW_PROPERTY(TAccountStatistics*, LocalStatisticsPtr);
    DEFINE_BYREF_RW_PROPERTY(TAccountStatistics, ClusterStatistics);
    DEFINE_BYREF_RW_PROPERTY(TClusterResourceLimits, ClusterResourceLimits);
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowChildrenLimitOvercommit);
    DEFINE_BYVAL_RW_PROPERTY(int, ChunkMergerNodeTraversalConcurrency, 1);
    DEFINE_BYREF_RW_PROPERTY(TChunkMergerCriteria, ChunkMergerCriteria);
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowUsingChunkMerger, true);

    //! Transient property.
    DEFINE_BYREF_RW_PROPERTY(TDetailedMasterMemory, DetailedMasterMemoryUsage);

    DEFINE_BYVAL_RW_PROPERTY(NObjectClient::TAbcConfigPtr, AbcConfig);
    DEFINE_BYVAL_RW_PROPERTY(std::optional<TString>, FolderId);

    DEFINE_BYREF_RO_PROPERTY(NConcurrency::IReconfigurableThroughputThrottlerPtr, MergeJobThrottler);

public:
    using TNonversionedMapObjectBase<TAccount>::TNonversionedMapObjectBase;
    TAccount(TAccountId id, bool isRoot = false);

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

    //! Dereferences the local statistics pointer.
    TAccountStatistics& LocalStatistics();

    //! Returns |true| if disk space limit is exceeded for at least one medium,
    //! i.e. no more disk space could be allocated.
    bool IsDiskSpaceLimitViolated() const;

    //! Returns |true| if disk space limit is exceeded for a given medium,
    //! i.e. no more disk space could be allocated.
    bool IsDiskSpaceLimitViolated(int mediumIndex) const;

    //! Returns |true| if node count limit is exceeded,
    //! i.e. no more Cypress node could be created.
    bool IsNodeCountLimitViolated() const;

    //! Returns |true| if chunk count limit is exceeded,
    //! i.e. no more chunks could be created.
    bool IsChunkCountLimitViolated() const;

    //! Returns |true| if tablet count limit is exceeded,
    //! i.e. no more tablets could be created.
    bool IsTabletCountLimitViolated() const;

    //! Returns |true| if tablet static memory limit is exceeded,
    //! i.e. no more data could be stored in tablet static memory.
    bool IsTabletStaticMemoryLimitViolated() const;

    //! Returns |true| if total master memory usage is exceeded,
    //! i.e. no more master memory can be occupied.
    bool IsTotalMasterMemoryLimitViolated() const;

    //! Returns |true| if master memory usage for specified cell is exceeded.
    bool IsCellMasterMemoryLimitViolated(NObjectClient::TCellTag cellTag) const;

    //! Returns |true| if chunk host master memory usage is exceeded.
    bool IsChunkHostMasterMemoryLimitViolated() const;

    //! Returns total chunk host master memory usage.
    i64 GetChunkHostCellMasterMemoryUsage() const;

    //! Returns statistics for a given cell tag.
    TAccountStatistics* GetCellStatistics(NObjectClient::TCellTag cellTag);

    void RecomputeClusterStatistics();

    //! Attaches a child account and adds its resource usage to its new ancestry.
    void AttachChild(const TString& key, TAccount* child) noexcept override;
    //! Unlinks a child account and subtracts its resource usage from its former ancestry.
    void DetachChild(TAccount* child) noexcept override;

    TClusterResourceLimits ComputeTotalChildrenLimits() const;

    TClusterResources ComputeTotalChildrenResourceUsage(bool committed) const;
    TAccountMulticellStatistics ComputeTotalChildrenMulticellStatistics() const;

    //! Returns violated limits of the account considering its ancestors.
    TViolatedClusterResourceLimits GetViolatedResourceLimits(
        NCellMaster::TBootstrap* bootstrap,
        bool enableTabletResourceValidation) const;
    //! Returns violated limits of the account considering its successors.
    TViolatedClusterResourceLimits GetRecursiveViolatedResourceLimits(
        NCellMaster::TBootstrap* bootstrap,
        bool enableTabletResourceValidation) const;

    int GetMergeJobRateLimit() const;
    void SetMergeJobRateLimit(int mergeJobRateLimit);

    int GetChunkMergerNodeTraversals() const;
    void IncrementChunkMergerNodeTraversals();
    void DecrementChunkMergerNodeTraversals();

private:
    int MergeJobRateLimit_ = 1;

    //! Indicates the number of nodes currently being processed by the chunk
    //! merger that belong to this account.
    NObjectServer::TEpochRefCounter ChunkMergerNodeTraversals_;

    TString GetRootName() const override;
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
