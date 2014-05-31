#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IDriver)

DECLARE_REFCOUNTED_CLASS(TDriverConfig)

struct TCommandDescriptor;

struct TDriverRequest;
struct TDriverResponse;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
