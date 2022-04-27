#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NNodeTrackerClient {

////////////////////////////////////////////////////////////////////////////////

struct INodeDirectorySynchronizer
    : public TRefCounted
{
    virtual void Start() const = 0;

    virtual TFuture<void> Stop() const = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeDirectorySynchronizer)

////////////////////////////////////////////////////////////////////////////////

INodeDirectorySynchronizerPtr CreateNodeDirectorySynchronizer(
    const NApi::NNative::IConnectionPtr& directoryConnection,
    TNodeDirectoryPtr nodeDirectory);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerClient
