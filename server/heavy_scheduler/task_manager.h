#pragma once

#include "public.h"

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TTaskManager
    : public TRefCounted
{
public:
    explicit TTaskManager(TTaskManagerConfigPtr config);
    ~TTaskManager();

    void RemoveFinishedTasks();

    void Add(ITaskPtr task, ETaskSource source);

    void ReconcileState(const NCluster::TClusterPtr& cluster);

    int GetTaskSlotCount(ETaskSource source) const;

    bool HasTaskInvolvingPod(NCluster::TPod* pod) const;

    int TaskCount() const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TTaskManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
