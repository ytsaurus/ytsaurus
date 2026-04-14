#pragma once

#include "public.h"

#include "porto_helpers.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/actions/future.h>
#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/net/address.h>

#include <library/cpp/porto/libporto.hpp>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeSpec
{
    TString Path;
    TString Backend;
    TString State;
};

////////////////////////////////////////////////////////////////////////////////

struct TRunnableContainerSpec
{
    TString Name;
    TString Command;

    EEnablePorto EnablePorto = EEnablePorto::None;
    bool Isolate = true;
    bool EnableFuse = false;

    std::optional<TString> StdinPath;
    std::optional<TString> StdoutPath;
    std::optional<TString> StderrPath;
    std::optional<TString> CurrentWorkingDirectory;
    std::optional<TString> CoreCommand;
    std::optional<TString> User;
    std::optional<int> GroupId;

    bool EnableCoreDumps = true;

    std::optional<double> CpuWeight;
    std::optional<i64> ThreadLimit;

    std::optional<TString> NetworkInterface;
    std::optional<TString> HostName;
    std::vector<NYT::NNet::TIP6Address> IPAddresses;
    bool EnableNat64 = false;
    bool DisableNetwork = false;

    THashMap<TString, TString> Labels;
    THashMap<TString, TString> Env;
    std::vector<TString> CGroupControllers;
    std::vector<TDevice> Devices;
    std::optional<TRootFS> RootFS;
    std::vector<TBind> Binds;

    //! Allowed places for creation of volumes and layers. "***" means any place.
    std::vector<TString> Places;
};

////////////////////////////////////////////////////////////////////////////////

struct IPortoExecutor
    : public TRefCounted
{
    virtual void OnDynamicConfigChanged(const TPortoExecutorDynamicConfigPtr& newConfig) = 0;

    virtual TFuture<void> CreateContainer(const TString& container) = 0;

    virtual TFuture<void> CreateContainer(const TRunnableContainerSpec& containerSpec, bool start) = 0;

    virtual TFuture<void> SetContainerProperty(
        const TString& container,
        const TString& property,
        const TString& value) = 0;

    virtual TFuture<std::optional<TString>> GetContainerProperty(
        const TString& container,
        const TString& property) = 0;

    virtual TFuture<THashMap<TString, TErrorOr<TString>>> GetContainerProperties(
        const TString& container,
        const std::vector<TString>& properties) = 0;
    virtual TFuture<THashMap<TString, THashMap<TString, TErrorOr<TString>>>> GetContainerProperties(
        const std::vector<TString>& containers,
        const std::vector<TString>& properties) = 0;

    virtual TFuture<THashMap<TString, i64>> GetContainerMetrics(
        const std::vector<TString>& containers,
        const TString& metric) = 0;
    virtual TFuture<void> DestroyContainer(const TString& container) = 0;
    virtual TFuture<void> StopContainer(const TString& container) = 0;
    virtual TFuture<void> StartContainer(const TString& container) = 0;
    virtual TFuture<void> KillContainer(const TString& container, int signal) = 0;
    virtual TFuture<void> RespawnContainer(const TString& container) = 0;

    virtual TFuture<TString> ConvertPath(const TString& path, const TString& container) = 0;

    // Returns absolute names of immediate children only.
    virtual TFuture<std::vector<TString>> ListSubcontainers(
        const TString& rootContainer,
        bool includeRoot) = 0;
    // Starts polling a given container, returns future with exit code of finished process.
    virtual TFuture<int> PollContainer(const TString& container) = 0;

    virtual TFuture<TString> CreateVolume(
        const TString& path,
        const THashMap<TString, TString>& properties) = 0;
    virtual TFuture<void> LinkVolume(
        const TString& path,
        const TString& name,
        const TString& target) = 0;
    virtual TFuture<void> UnlinkVolume(
        const TString& path,
        const TString& name,
        const TString& target = AnyTarget) = 0;
    virtual TFuture<std::vector<TString>> ListVolumePaths() = 0;
    virtual TFuture<std::vector<TVolumeSpec>> GetVolumes() = 0;

    virtual TFuture<void> ImportLayer(
        const TString& archivePath,
        const TString& layerId,
        const TString& place,
        const TString& container) = 0;
    virtual TFuture<void> RemoveLayer(
        const TString& layerId,
        const TString& place,
        bool async) = 0;
    virtual TFuture<std::vector<TString>> ListLayers(const TString& place) = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TError&), Failed);
};

DEFINE_REFCOUNTED_TYPE(IPortoExecutor)

////////////////////////////////////////////////////////////////////////////////

IPortoExecutorPtr CreatePortoExecutor(
    TPortoExecutorDynamicConfigPtr config,
    std::string threadNameSuffix,
    const NProfiling::TProfiler& profiler = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainers
