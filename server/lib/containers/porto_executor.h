#pragma once

#ifndef __linux__
#error Platform must be linux to include this
#endif

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/signal.h>

#include <yt/core/profiling/profiler.h>

#include <infra/porto/api/libporto.hpp>
#include <infra/porto/proto/rpc.pb.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeId
{
    TString Path;
};

////////////////////////////////////////////////////////////////////////////////

const int PortoErrorCodeBase = 12000;

DEFINE_ENUM(EPortoErrorCode,
    ((Success)                       ((PortoErrorCodeBase + Porto::EError::Success)))
    ((Unknown)                       ((PortoErrorCodeBase + Porto::EError::Unknown)))
    ((InvalidMethod)                 ((PortoErrorCodeBase + Porto::EError::InvalidMethod)))
    ((ContainerAlreadyExists)        ((PortoErrorCodeBase + Porto::EError::ContainerAlreadyExists)))
    ((ContainerDoesNotExist)         ((PortoErrorCodeBase + Porto::EError::ContainerDoesNotExist)))
    ((InvalidProperty)               ((PortoErrorCodeBase + Porto::EError::InvalidProperty)))
    ((InvalidData)                   ((PortoErrorCodeBase + Porto::EError::InvalidData)))
    ((InvalidValue)                  ((PortoErrorCodeBase + Porto::EError::InvalidValue)))
    ((InvalidState)                  ((PortoErrorCodeBase + Porto::EError::InvalidState)))
    ((NotSupported)                  ((PortoErrorCodeBase + Porto::EError::NotSupported)))
    ((ResourceNotAvailable)          ((PortoErrorCodeBase + Porto::EError::ResourceNotAvailable)))
    ((Permission)                    ((PortoErrorCodeBase + Porto::EError::Permission)))
    ((VolumeAlreadyExists)           ((PortoErrorCodeBase + Porto::EError::VolumeAlreadyExists)))
    ((VolumeNotFound)                ((PortoErrorCodeBase + Porto::EError::VolumeNotFound)))
    ((NoSpace)                       ((PortoErrorCodeBase + Porto::EError::NoSpace)))
    ((Busy)                          ((PortoErrorCodeBase + Porto::EError::Busy)))
    ((VolumeAlreadyLinked)           ((PortoErrorCodeBase + Porto::EError::VolumeAlreadyLinked)))
    ((VolumeNotLinked)               ((PortoErrorCodeBase + Porto::EError::VolumeNotLinked)))
    ((LayerAlreadyExists)            ((PortoErrorCodeBase + Porto::EError::LayerAlreadyExists)))
    ((LayerNotFound)                 ((PortoErrorCodeBase + Porto::EError::LayerNotFound)))
    ((NoValue)                       ((PortoErrorCodeBase + Porto::EError::NoValue)))
    ((VolumeNotReady)                ((PortoErrorCodeBase + Porto::EError::VolumeNotReady)))
    ((InvalidCommand)                ((PortoErrorCodeBase + Porto::EError::InvalidCommand)))
    ((LostError)                     ((PortoErrorCodeBase + Porto::EError::LostError)))
    ((DeviceNotFound)                ((PortoErrorCodeBase + Porto::EError::DeviceNotFound)))
    ((InvalidPath)                   ((PortoErrorCodeBase + Porto::EError::InvalidPath)))
    ((InvalidNetworkAddress)         ((PortoErrorCodeBase + Porto::EError::InvalidNetworkAddress)))
    ((PortoFrozen)                   ((PortoErrorCodeBase + Porto::EError::PortoFrozen)))
    ((LabelNotFound)                 ((PortoErrorCodeBase + Porto::EError::LabelNotFound)))
    ((InvalidLabel)                  ((PortoErrorCodeBase + Porto::EError::InvalidLabel)))
    ((NotFound)                      ((PortoErrorCodeBase + Porto::EError::NotFound)))
    ((SocketError)                   ((PortoErrorCodeBase + Porto::EError::SocketError)))
    ((SocketUnavailable)             ((PortoErrorCodeBase + Porto::EError::SocketUnavailable)))
    ((SocketTimeout)                 ((PortoErrorCodeBase + Porto::EError::SocketTimeout)))
    ((Taint)                         ((PortoErrorCodeBase + Porto::EError::Taint)))
    ((Queued)                        ((PortoErrorCodeBase + Porto::EError::Queued)))
);

////////////////////////////////////////////////////////////////////////////////

struct IPortoExecutor
    : public TRefCounted
{
    virtual TFuture<void> CreateContainer(const TString& name) = 0;
    virtual TFuture<void> SetContainerProperty(
        const TString& name,
        const TString& key,
        const TString& value) = 0;
    virtual TFuture<std::map<TString, TErrorOr<TString>>> GetContainerProperties(
        const TString& name,
        const std::vector<TString>& value) = 0;
    virtual TFuture<void> DestroyContainer(const TString& name) = 0;
    virtual TFuture<void> StopContainer(const TString& name) = 0;
    virtual TFuture<void> StartContainer(const TString& name) = 0;
    virtual TFuture<void> KillContainer(const TString& name, int signal) = 0;
    virtual TFuture<std::vector<TString>> ListContainers() = 0;
    // Starts polling a given container, returns future with exit code of finished process.
    virtual TFuture<int> PollContainer(const TString& name) = 0;

    virtual TFuture<TString> CreateVolume(
        const TString& path,
        const std::map<TString, TString>& properties) = 0;
    virtual TFuture<void> LinkVolume(
        const TString& path,
        const TString& name) = 0;
    virtual TFuture<void> UnlinkVolume(
        const TString& path,
        const TString& name) = 0;
    virtual TFuture<std::vector<TString>> ListVolumePaths() = 0;

    virtual TFuture<void> ImportLayer(
        const TString& archivePath,
        const TString& layerId,
        const TString& place) = 0;
    virtual TFuture<void> RemoveLayer(
        const TString& layerId,
        const TString& place) = 0;
    virtual TFuture<std::vector<TString>> ListLayers(const TString& place) = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TError&), Failed)
};

DEFINE_REFCOUNTED_TYPE(IPortoExecutor)

////////////////////////////////////////////////////////////////////////////////

IPortoExecutorPtr CreatePortoExecutor(
    TPortoExecutorConfigPtr config,
    const TString& threadNameSuffix,
    const NProfiling::TProfiler& profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
