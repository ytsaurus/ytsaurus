#pragma once

#include "election_manager.h"

#include <yt/yt/core/misc/atomic_object.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerThunk
    : public IElectionManager
{
public:
    void Initialize() override;
    void Finalize() override;

    void Participate() override;
    void Abandon(const TError& error) override;
    void ReconfigureCell(TCellManagerPtr cellManager) override;

    NYson::TYsonProducer GetMonitoringProducer() override;
    TPeerIdSet GetAlivePeerIds() override;

    void SetUnderlying(IElectionManagerPtr underlying);

private:
    TAtomicObject<IElectionManagerPtr> Underlying_;

};

DEFINE_REFCOUNTED_TYPE(TElectionManagerThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
