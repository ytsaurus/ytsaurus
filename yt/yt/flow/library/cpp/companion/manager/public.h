#pragma once

#include <yt/yt/flow/library/cpp/companion/client/public.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

//! Alias under which companion computations and companion resources expect
//! the companion manager among their required resources and dependencies.
constexpr const char CompanionManagerAlias[] = "CompanionManager";

DECLARE_REFCOUNTED_CLASS(TCompanionManager);
DECLARE_REFCOUNTED_CLASS(TJavaCompanionManager);

DECLARE_REFCOUNTED_STRUCT(TCompanionEntrypoint);
DECLARE_REFCOUNTED_STRUCT(TCompanionManagerParameters);
DECLARE_REFCOUNTED_STRUCT(TJavaCompanionManagerParameters);

DECLARE_REFCOUNTED_CLASS(TJavaProcessManager);
DECLARE_REFCOUNTED_CLASS(TProcessManagerBase);
DECLARE_REFCOUNTED_CLASS(TCompanionProcessManager);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
