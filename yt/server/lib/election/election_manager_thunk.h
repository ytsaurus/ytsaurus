#pragma once

#include "election_manager.h"

#include <yt/core/misc/atomic_object.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerThunk
    : public IElectionManager
{
public:
    virtual void Initialize() override;
    virtual void Finalize() override;

    virtual void Participate() override;
    virtual void Abandon(const TError& error) override;
    virtual void ReconfigureCell(TCellManagerPtr cellManager) override;

    virtual NYson::TYsonProducer GetMonitoringProducer();

    void SetUnderlying(IElectionManagerPtr underlying);

private:
    TAtomicObject<IElectionManagerPtr> Underlying_;

};

DEFINE_REFCOUNTED_TYPE(TElectionManagerThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
