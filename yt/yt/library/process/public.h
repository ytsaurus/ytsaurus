#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNamedPipe)
DECLARE_REFCOUNTED_STRUCT(TNamedPipeConfig)

DECLARE_REFCOUNTED_STRUCT(TIODispatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TIODispatcherDynamicConfig)


YT_DECLARE_RECONFIGURABLE_SINGLETON(TIODispatcherConfig, TIODispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
