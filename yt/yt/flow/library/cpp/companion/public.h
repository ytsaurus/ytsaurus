#pragma once

#include <yt/yt/flow/library/cpp/companion/client/public.h>
#include <yt/yt/flow/library/cpp/companion/manager/public.h>
#include <yt/yt/flow/library/cpp/companion/resources/public.h>

namespace NYT::NFlow::NCompanion {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TSwiftMapCompanionComputation);
DECLARE_REFCOUNTED_CLASS(TSwiftOrderedSourceCompanionComputation);
DECLARE_REFCOUNTED_CLASS(TTransformCompanionComputation);
DECLARE_REFCOUNTED_CLASS(TTransformOrderedSourceCompanionComputation);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NCompanion
