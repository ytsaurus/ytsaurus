#include "stdafx.h"
#include "world_initializer.h"

#include "../misc/periodic_invoker.h"

namespace NYT {
namespace NCypress {

using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

TWorldInitializer::TWorldInitializer(
    NMetaState::TMetaStateManager* metaStateManager,
    TCypressManager* cypressManager)
    : MetaStateManager(metaStateManager)
    , CypressManager(cypressManager)
{
    YASSERT(metaStateManager != NULL);
    YASSERT(cypressManager != NULL);
}

void TWorldInitializer::Start()
{
    auto invoker = New<TPeriodicInvoker>(
        FromMethod(&TWorldInitializer::CheckWorldInitialized, TPtr(this))
        ->Via(MetaStateManager->GetStateInvoker()),
        TDuration::Seconds(3));
    invoker->Start();
}

void TWorldInitializer::CheckWorldInitialized()
{
    if (MetaStateManager->GetStateStatus() != EPeerStatus::Leading)
        return;

    if (CypressManager->IsWorldInitialized())
        return;

    CypressManager
        ->InitiateCreateWorld()
        ->Commit();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

