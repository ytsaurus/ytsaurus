#pragma once

#include "public.h"

#include <ytlib/cell_scheduler/public.h>
#include <ytlib/rpc/service.h>
#include <ytlib/ytree/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

class TScheduler
    : public TRefCounted
{
public:
    TScheduler(
        TSchedulerConfigPtr config,
        NCellScheduler::TBootstrap* bootstrap);

    ~TScheduler();

    void Start();

    NRpc::IService::TPtr GetService();
    NYTree::TYPathServiceProducer CreateOrchidProducer();

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

