#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TStateManagerMock);
DECLARE_REFCOUNTED_CLASS(TMutableStateProviderMock);
DECLARE_REFCOUNTED_CLASS(TInitContextMock);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
