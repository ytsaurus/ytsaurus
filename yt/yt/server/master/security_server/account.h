#pragma once

#include "public.h"
#include "acl.h"
#include "cluster_resources.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/map_object.h>
#include <yt/server/master/object_server/object.h>

#include <yt/core/yson/public.h>

#include <yt/core/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

struct TAccountStatistics
{
    TClusterResources ResourceUsage;
    TClusterResources CommittedResourceUsage;

    void Persist(NCellMaster::TPersistenceContext& context);
};

void ToProto(NProto::TAccountStatistics* protoStatistics, const TAccountStatistics& statistics);
void FromProto(TAccountStatistics* statistics, const NProto::TAccountStatistics& protoStatistics);

void Serialize(const TAccountStatistics& statistics, NYson::IYsonConsumer* consumer, const NChunkServer::TChunkManagerPtr& chunkManager);

TAccountStatistics& operator += (TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics  operator +  (const TAccountStatistics& lhs, const TAccountStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

class TAccount
    : public NObjectServer::TNonversionedMapObjectBase<TAccount>
{
public:
    using TMulticellStatistics = THashMap<NObjectClient::TCellTag, TAccountStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TMulticellStatistics, MulticellStatistics);
    DEFINE_BYVAL_RW_PROPERTY(TAccountStatistics*, LocalStatisticsPtr);
    DEFINE_BYREF_RW_PROPERTY(TAccountStatistics, ClusterStatistics);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, ClusterResourceLimits);
    DEFINE_BYVAL_RW_PROPERTY(bool, AllowChildrenLimitOvercommit);

    // COMPAT(kiselyovp)
    DEFINE_BYVAL_RW_PROPERTY(TString, LegacyName);

    //! Transient property.
    DEFINE_BYVAL_RW_PROPERTY(i64, MasterMemoryUsage);

public:
    explicit TAccount(TAccountId id, bool isRoot = false);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

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

    //! Returns |true| if master memory usage is exceeded,
    //! i.e. no more master memory can be occupied.
    bool IsMasterMemoryLimitViolated() const;

    //! Returns statistics for a given cell tag.
    TAccountStatistics* GetCellStatistics(NObjectClient::TCellTag cellTag);

    void RecomputeClusterStatistics();

    //! Attaches a child account and adds its resource usage to its new ancestry.
    virtual void AttachChild(const TString& key, TAccount* child) noexcept override;
    //! Unlinks a child account and subtracts its resource usage from its former ancestry.
    virtual void DetachChild(TAccount* child) noexcept override;

    TClusterResources ComputeTotalChildrenLimits() const;

    TClusterResources ComputeTotalChildrenResourceUsage() const;
    TClusterResources ComputeTotalChildrenCommittedResourceUsage() const;

private:
    virtual TString GetRootName() const override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
