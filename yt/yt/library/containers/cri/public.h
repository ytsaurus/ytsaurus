#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCriPodSpec)
DECLARE_REFCOUNTED_STRUCT(TCriContainerSpec)
DECLARE_REFCOUNTED_CLASS(TCriExecutorConfig)
DECLARE_REFCOUNTED_CLASS(TCriAuthConfig)
DECLARE_REFCOUNTED_STRUCT(ICriExecutor)

DECLARE_REFCOUNTED_CLASS(TCriImageCacheConfig)
DECLARE_REFCOUNTED_CLASS(TCriImageCacheEntry)
DECLARE_REFCOUNTED_STRUCT(ICriImageCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
