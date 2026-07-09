#pragma once

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TRandomSourceParameters);
DECLARE_REFCOUNTED_CLASS(TRandomSource);
DECLARE_REFCOUNTED_CLASS(TRandomSourceController);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
