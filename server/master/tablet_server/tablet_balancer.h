#pragma once

#include "public.h"

#include <yt/server/master/cell_master/public.h>
#include <yt/server/master/table_server/public.h>

#include <yt/core/actions/future.h>

namespace NYT::NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletBalancer
    : public TRefCounted
{
public:
    TTabletBalancer(
        TTabletBalancerMasterConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TTabletBalancer();

    void Start();
    void Stop();

    void OnTabletHeartbeat(TTablet* tablet);

    std::vector<TTabletActionId> SyncBalanceCells(
        TTabletCellBundle* bundle,
        const std::optional<std::vector<NTableServer::TTableNode*>>& tables);

    std::vector<TTabletActionId> SyncBalanceTablets(NTableServer::TTableNode* table);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTabletBalancer)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletServer
