#pragma once

#include "public.h"
#include "acl.h"
#include "cluster_resources.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/object.h>

#include <yt/core/yson/public.h>

#include <yt/core/misc/property.h>

namespace NYT {
namespace NSecurityServer {

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
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

    using TMulticellStatistics = THashMap<NObjectClient::TCellTag, TAccountStatistics>;
    DEFINE_BYREF_RW_PROPERTY(TMulticellStatistics, MulticellStatistics);
    DEFINE_BYVAL_RW_PROPERTY(TAccountStatistics*, LocalStatisticsPtr);
    DEFINE_BYREF_RW_PROPERTY(TAccountStatistics, ClusterStatistics);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, ClusterResourceLimits);

    DEFINE_BYREF_RW_PROPERTY(TAccessControlDescriptor, Acd);

public:
    explicit TAccount(const TAccountId& id);

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

    //! Returns |true| is node count limit is exceeded,
    //! i.e. no more Cypress node could be created.
    bool IsNodeCountLimitViolated() const;

    //! Returns |true| is chunk count limit is exceeded,
    //! i.e. no more chunks could be created.
    bool IsChunkCountLimitViolated() const;

    //! Returns |true| is tablet count limit is exceeded,
    //! i.e. no more tablets could be created.
    bool IsTabletCountLimitViolated() const;

    //! Returns |true| is tablet static memory limit is exceeded,
    //! i.e. no more data could be stored in tablet static memory.
    bool IsTabletStaticMemoryLimitViolated() const;

    //! Returns statistics for a given cell tag.
    TAccountStatistics* GetCellStatistics(NObjectClient::TCellTag cellTag);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
