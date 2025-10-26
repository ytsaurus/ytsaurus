#pragma once

#include <yt/yt/core/misc/configurable_singleton_decl.h>

#include <library/cpp/yt/memory/ref_counted.h>

namespace NYT::NPipeIO {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNamedPipe)
DECLARE_REFCOUNTED_STRUCT(TNamedPipeConfig)

DECLARE_REFCOUNTED_STRUCT(TPipeIODispatcherConfig)
DECLARE_REFCOUNTED_STRUCT(TPipeIODispatcherDynamicConfig)

YT_DECLARE_RECONFIGURABLE_SINGLETON(TPipeIODispatcherConfig, TPipeIODispatcherDynamicConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipeIO
