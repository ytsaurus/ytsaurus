#pragma once

#include <core/misc/public.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDriver)

DECLARE_REFCOUNTED_CLASS(TDriverConfig)

struct TCommandDescriptor;
struct TDriverRequest;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
