#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/server/master/public.h>

#include <yp/server/lib/cluster/public.h>

#include <yt/core/rpc/public.h>

namespace NYP::NServer::NAccounting {

////////////////////////////////////////////////////////////////////////////////

class TAccountingManager
    : public TRefCounted
{
public:
    TAccountingManager(
        NMaster::TBootstrap* bootstrap,
        TAccountingManagerConfigPtr config);
    ~TAccountingManager();

    void Initialize();

    void PrepareValidateAccounting(NObjects::TPod* pod);
    void ValidateAccounting(const std::vector<NObjects::TPod*>& pods);

    void UpdateNodeSegmentsStatus(const NCluster::TClusterPtr& cluster);
    void UpdateAccountsStatus(const NCluster::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAccountingManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccessControl
