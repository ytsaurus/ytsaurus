#pragma once

#include "common.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriverHost;

struct IDriver;
typedef TIntrusivePtr<IDriver> IDriverPtr;

struct TDriverConfig;
typedef TIntrusivePtr<TDriverConfig> TDriverConfigPtr;

struct TCommandDescriptor;

struct ICommand;
typedef TIntrusivePtr<ICommand> ICommandPtr;

struct ICommandHost;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
