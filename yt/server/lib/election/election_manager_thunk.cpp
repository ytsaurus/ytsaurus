#include "election_manager_thunk.h"

#include <yt/core/yson/producer.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

void TElectionManagerThunk::Initialize()
{
    Underlying_.Load()->Initialize();
}

void TElectionManagerThunk::Finalize()
{
    Underlying_.Load()->Finalize();
}

void TElectionManagerThunk::Participate()
{
    Underlying_.Load()->Participate();
}

void TElectionManagerThunk::Abandon(const TError& error)
{
    Underlying_.Load()->Abandon(error);
}

void TElectionManagerThunk::ReconfigureCell(TCellManagerPtr cellManager)
{
    Underlying_.Load()->ReconfigureCell(std::move(cellManager));
}

NYson::TYsonProducer TElectionManagerThunk::GetMonitoringProducer()
{
    return Underlying_.Load()->GetMonitoringProducer();
}

void TElectionManagerThunk::SetUnderlying(IElectionManagerPtr underlying)
{
    Underlying_.Store(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

