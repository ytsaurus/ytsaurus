#pragma once

#include "election_manager.h"

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerThunk
    : public IElectionManager
{
public:
    void Initialize() override;
    void Finalize() override;

    void Participate() override;
    TFuture<void> Abandon(const TError& error) override;
    void ReconfigureCell(TCellManagerPtr cellManager) override;

    NYson::TYsonProducer GetMonitoringProducer() override;
    TPeerIdSet GetAlivePeerIds() override;

    TCellManagerPtr GetCellManager() override;

    void SetUnderlying(IElectionManagerPtr underlying);

private:
    TAtomicIntrusivePtr<IElectionManager> Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TElectionManagerThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
