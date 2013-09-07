#pragma once

#include <ytlib/misc/common.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriver;
typedef TIntrusivePtr<IDriver> IDriverPtr;

class TDriverConfig;
typedef TIntrusivePtr<TDriverConfig> TDriverConfigPtr;

struct TCommandDescriptor;

struct TDriverRequest;
struct TDriverResponse;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
