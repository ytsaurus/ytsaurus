#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class THeavyScheduler
    : public TRefCounted
{
public:
    THeavyScheduler(TBootstrap* bootstrap, THeavySchedulerConfigPtr config);

    void Initialize();

    const TTaskManagerPtr& GetTaskManager() const;
    const TDisruptionThrottlerPtr& GetDisruptionThrottler() const;
    const NClient::NApi::NNative::IClientPtr& GetClient() const;
    const TObjectId& GetNodeSegment() const;
    bool GetVerbose() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(THeavyScheduler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
