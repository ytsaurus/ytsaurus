#include "stdafx.h"
#include "world_initializer.h"

#include <ytlib/actions/action_util.h>
#include <ytlib/cypress/cypress_manager.h>
#include <ytlib/logging/log.h>

namespace NYT {
namespace NCypress {

using namespace NCellMaster;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static TDuration CheckPeriod = TDuration::Seconds(1);
static NLog::TLogger Logger("Cypress");

////////////////////////////////////////////////////////////////////////////////

TWorldInitializer::TWorldInitializer(TBootstrap* bootstrap)
    : Bootstrap(bootstrap)
{
    YASSERT(bootstrap);

    PeriodicInvoker = New<TPeriodicInvoker>(
        // TODO(babenko): use AsWeak
        FromMethod(&TWorldInitializer::OnCheck, TWeakPtr<TWorldInitializer>(this))
        ->Via(bootstrap->GetStateInvoker()),
        CheckPeriod);
    PeriodicInvoker->Start();
}

bool TWorldInitializer::IsInitialized() const
{
    // 1 means just the root.
    return Bootstrap->GetCypressManager()->GetNodeCount() > 1;
}

void TWorldInitializer::OnCheck()
{
    if (IsInitialized()) {
        PeriodicInvoker->Stop();
    } else if (CanInitialize()) {
        Initialize();
        PeriodicInvoker->Stop();
    }
}

bool TWorldInitializer::CanInitialize() const
{
    return
        Bootstrap->GetMetaStateManager()->GetStateStatus() == EPeerStatus::Leading &&
        Bootstrap->GetMetaStateManager()->HasActiveQuorum();
}

void TWorldInitializer::Initialize()
{
    LOG_INFO("World initialization started");

    try {

    } catch (const std::exception& ex) {
        LOG_FATAL("World initialization failed\n%s", ex.what());
    }

    LOG_INFO("World initialization completed");

    PeriodicInvoker->Stop();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypress
} // namespace NYT

