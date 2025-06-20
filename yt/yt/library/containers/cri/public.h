#pragma once

#include <library/cpp/yt/memory/intrusive_ptr.h>

namespace NYT::NContainers::NCri {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCriPodSpec)
DECLARE_REFCOUNTED_STRUCT(TCriContainerSpec)
DECLARE_REFCOUNTED_STRUCT(TCriExecutorConfig)
DECLARE_REFCOUNTED_STRUCT(TCriAuthConfig)
DECLARE_REFCOUNTED_STRUCT(ICriExecutor)

DECLARE_REFCOUNTED_STRUCT(TCriImageCacheConfig)
DECLARE_REFCOUNTED_CLASS(TCriImageCacheEntry)
DECLARE_REFCOUNTED_STRUCT(ICriImageCache)
DECLARE_REFCOUNTED_STRUCT(TCriPodDescriptor)
DECLARE_REFCOUNTED_STRUCT(TCriContainerResources)
DECLARE_REFCOUNTED_STRUCT(TCriPodSpec)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers::NCri
