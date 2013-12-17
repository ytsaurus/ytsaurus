#pragma once

#include <core/misc/common.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriver;
typedef TIntrusivePtr<IDriver> IDriverPtr;

class TQueryCallbacksProvider;
typedef TIntrusivePtr<TQueryCallbacksProvider> TQueryCallbacksProviderPtr;

class TDriverConfig;
typedef TIntrusivePtr<TDriverConfig> TDriverConfigPtr;

struct TCommandDescriptor;

struct TDriverRequest;
struct TDriverResponse;

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT
