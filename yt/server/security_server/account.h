#pragma once

#include "public.h"
#include "cluster_resources.h"
#include "acl.h"

#include <core/misc/property.h>

#include <core/yson/public.h>

#include <server/object_server/object.h>

#include <server/cell_master/public.h>

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

void Serialize(const TAccountStatistics& statistics, NYson::IYsonConsumer* consumer);

TAccountStatistics& operator += (TAccountStatistics& lhs, const TAccountStatistics& rhs);
TAccountStatistics  operator +  (const TAccountStatistics& lhs, const TAccountStatistics& rhs);

////////////////////////////////////////////////////////////////////////////////

class TAccount
    : public NObjectServer::TNonversionedObjectBase
{
public:
    DEFINE_BYVAL_RW_PROPERTY(Stroka, Name);

    using TMulticellStatistics = yhash_map<NObjectClient::TCellTag, TAccountStatistics>;
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

    //! Returns |true| if disk space limit is exceeded,
    //! i.e. no more disk space could be allocated.
    bool IsDiskSpaceLimitViolated() const;

    //! Returns |true| is node count limit is exceeded,
    //! i.e. no more Cypress node could be created.
    bool IsNodeCountLimitViolated() const;

    //! Returns |true| is chunk count limit is exceeded,
    //! i.e. no more chunks could be created.
    bool IsChunkCountLimitViolated() const;

    //! Throws if account limit is exceeded for some resource type with positive delta.
    void ValidateResourceUsageIncrease(const TClusterResources& delta);

    //! Returns statistics for a given cell tag.
    TAccountStatistics* GetCellStatistics(NObjectClient::TCellTag cellTag);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NSecurityServer
} // namespace NYT
