#include "election_manager_thunk.h"

#include <yt/core/yson/producer.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

void TElectionManagerThunk::Initialize()
{
    Underlying_->Initialize();
}

void TElectionManagerThunk::Finalize()
{
    Underlying_->Finalize();
}

void TElectionManagerThunk::Participate()
{
    Underlying_->Participate();
}

void TElectionManagerThunk::Abandon(const TError& error)
{
    Underlying_->Abandon(error);
}

NYson::TYsonProducer TElectionManagerThunk::GetMonitoringProducer()
{
    return Underlying_->GetMonitoringProducer();
}

void TElectionManagerThunk::SetUnderlying(IElectionManagerPtr underlying)
{
    Underlying_ = std::move(underlying);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

