#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

#include <yt/contrib/portoapi/rpc.pb.h>
#include <yt/contrib/portoapi/libporto.hpp>

namespace NYT {
namespace NContainers {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeID
{
    Stroka Path;
};

////////////////////////////////////////////////////////////////////////////////

const int ContainerErrorCodeBase = 12000;

DEFINE_ENUM(EContainerErrorCode,
    ((InvalidState)((ContainerErrorCodeBase + ::rpc::EError::InvalidState)))
);

////////////////////////////////////////////////////////////////////////////////

struct IPortoExecutor
    : public TRefCounted
{
    virtual TFuture<void> CreateContainer(const Stroka& name) = 0;
    virtual TFuture<void> SetProperty(
        const Stroka& name,
        const Stroka& key,
        const Stroka& value) = 0;
    virtual TFuture<std::map<Stroka, TErrorOr<Stroka>>> GetProperties(
        const Stroka& name,
        const std::vector<Stroka>& value) = 0;
    virtual TFuture<void> DestroyContainer(const Stroka& name) = 0;
    virtual TFuture<void> Start(const Stroka& name) = 0;
    virtual TFuture<void> Kill(const Stroka& name, int signal) = 0;
    virtual TFuture<std::vector<Stroka>> ListContainers() = 0;
    // Starts polling a given container, returns future with exit code of finished process.
    virtual TFuture<int> AsyncPoll(const Stroka& name) = 0;
    virtual TFuture<TVolumeID> CreateVolume(
        const Stroka& path,
        const std::map<Stroka, Stroka>& properties) = 0;
    virtual TFuture<void> LinkVolume(
        const Stroka& path,
        const Stroka& name) = 0;
    virtual TFuture<void> UnlinkVolume(
        const Stroka& path,
        const Stroka& name) = 0;
    virtual TFuture<std::vector<Porto::Volume>> ListVolumes() = 0;
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Failed)
};

DEFINE_REFCOUNTED_TYPE(IPortoExecutor)

////////////////////////////////////////////////////////////////////////////////

IPortoExecutorPtr CreatePortoExecutor(
    TDuration retryTime = TDuration::Seconds(10),
    TDuration pollPeriod = TDuration::MilliSeconds(100));

////////////////////////////////////////////////////////////////////////////////

} // namespace NContainers
} // namespce NYT
