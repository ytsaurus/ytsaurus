#pragma once

#include "private.h"

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

struct IDriver;
typedef TIntrusivePtr<IDriver> IDriverPtr;

struct TDriverConfig;
typedef TIntrusivePtr<TDriverConfig> TDriverConfigPtr;

struct TCommandDescriptor;

struct TDriverRequest;
struct TDriverResponse;

struct TFormat;

struct TTsvWriterConfig;
typedef TIntrusivePtr<TTsvWriterConfig> TTsvWriterConfigPtr;

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NDriver
} // namespace NYT
