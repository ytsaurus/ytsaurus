#pragma once

#include "public.h"

#include <yp/server/objects/public.h>

#include <yp/server/master/public.h>

#include <yp/server/scheduler/public.h>

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

    void Initialize();

    void PrepareValidateAccounting(NObjects::TPod* pod);
    void ValidateAccounting(const std::vector<NObjects::TPod*>& pods);

    void UpdateNodeSegmentsStatus(const NScheduler::TClusterPtr& cluster);
    void UpdateAccountsStatus(const NScheduler::TClusterPtr& cluster);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TAccountingManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NAccessControl
