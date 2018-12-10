#pragma once

#include "election_manager.h"

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

class TElectionManagerThunk
    : public IElectionManager
{
public:
    virtual void Initialize() override;
    virtual void Finalize() override;

    virtual void Participate() override;
    virtual void Abandon() override;

    virtual NYson::TYsonProducer GetMonitoringProducer();

    void SetUnderlying(IElectionManagerPtr underlying);

private:
    IElectionManagerPtr Underlying_;

};

DEFINE_REFCOUNTED_TYPE(TElectionManagerThunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection
