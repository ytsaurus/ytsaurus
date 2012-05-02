#pragma once

#include "common.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriver;
typedef TIntrusivePtr<IDriver> IDriverPtr;

struct TDriverConfig;
typedef TIntrusivePtr<TDriverConfig> TDriverConfigPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
