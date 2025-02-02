#pragma once

#include "public.h"
#include "cluster_resource_limits.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/map_object.h>
#include <yt/yt/server/master/object_server/object.h>
#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/master/transaction_server/public.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TAccountResourceUsageLease
    : public NObjectServer::TObject
    , public TRefTracked<TAccountResourceUsageLease>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TInstant, CreationTime);
    DEFINE_BYVAL_RW_PROPERTY(NTransactionServer::TTransactionRawPtr, Transaction);
    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccountPtr, Account);
    DEFINE_BYREF_RW_PROPERTY(TClusterResources, Resources);

public:
    // NB: It is necessary for LoadKeys in EntityMap.
    using TObject::TObject;

    TAccountResourceUsageLease(
        TAccountResourceUsageLeaseId id,
        NTransactionServer::TTransaction* transaction,
        NSecurityServer::TAccount* account);

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TAccountResourceUsageLease)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
