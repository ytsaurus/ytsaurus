#pragma once

#include "public.h"

#include <yt/yt/orm/server/master/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/concurrency/two_level_fair_share_thread_pool.h>

namespace NYT::NOrm::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

struct IPoolWeightManager
    : public NConcurrency::IPoolWeightProvider
{
public:
    virtual ~IPoolWeightManager() = default;

    virtual bool IsFairShareEnabled() const = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TPoolWeightManagerConfigPtr&), ConfigUpdate);
};

DEFINE_REFCOUNTED_TYPE(IPoolWeightManager);

////////////////////////////////////////////////////////////////////////////////

IPoolWeightManagerPtr CreatePoolWeightManager(
    NMaster::IBootstrap* bootstrap,
    TPoolWeightManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NObjects
