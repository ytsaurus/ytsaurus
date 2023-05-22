#pragma once

#include "io_engine.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct IDynamicIOEngine
    : public IIOEngine
{
    virtual void SetType(EIOEngineType type) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDynamicIOEngine)

////////////////////////////////////////////////////////////////////////////////

IDynamicIOEnginePtr CreateDynamicIOEngine(
    EIOEngineType defaultEngineType,
    NYTree::INodePtr ioConfig,
    bool enableUring,
    TString locationId = "default",
    NProfiling::TProfiler profiler = {},
    NLogging::TLogger logger = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
