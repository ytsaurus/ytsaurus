#include "election_manager_thunk.h"

#include <yt/yt/core/actions/future.h>

#include <yt/yt/core/yson/producer.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

void TElectionManagerThunk::Initialize()
{
    Underlying_.Acquire()->Initialize();
}

void TElectionManagerThunk::Finalize()
{
    Underlying_.Acquire()->Finalize();
}

void TElectionManagerThunk::Participate()
{
    Underlying_.Acquire()->Participate();
}

TFuture<void> TElectionManagerThunk::Abandon(const TError& error)
{
    return Underlying_.Acquire()->Abandon(error);
}

void TElectionManagerThunk::ReconfigureCell(TCellManagerPtr cellManager)
{
    Underlying_.Acquire()->ReconfigureCell(std::move(cellManager));
}

NYson::TYsonProducer TElectionManagerThunk::GetMonitoringProducer()
{
    return Underlying_.Acquire()->GetMonitoringProducer();
}

TPeerIdSet TElectionManagerThunk::GetAlivePeerIds()
{
    return Underlying_.Acquire()->GetAlivePeerIds();
}

TCellManagerPtr TElectionManagerThunk::GetCellManager()
{
    return Underlying_.Acquire()->GetCellManager();
}

void TElectionManagerThunk::SetUnderlying(IElectionManagerPtr underlying)
{
    Underlying_.Store(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

