#include "election_manager.h"

#include <yt/core/actions/cancelable_context.h>

namespace NYT::NElection {

////////////////////////////////////////////////////////////////////////////////

TEpochContext::TEpochContext(TCellManagerPtr cellManager)
    : CellManager(std::move(cellManager))
    , CancelableContext(New<TCancelableContext>())
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NElection

