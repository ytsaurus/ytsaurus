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
    std::string Path;
    std::string Backend;
    std::string State;
};

////////////////////////////////////////////////////////////////////////////////

struct TRunnableContainerSpec
{
    std::string Name;
    std::string Command;

    EEnablePorto EnablePorto = EEnablePorto::None;
    bool Isolate = true;
    bool EnableFuse = false;

    std::optional<std::string> StdinPath;
    std::optional<std::string> StdoutPath;
    std::optional<std::string> StderrPath;
    std::optional<std::string> CurrentWorkingDirectory;
    std::optional<std::string> CoreCommand;
    std::optional<std::string> User;
    std::optional<int> GroupId;

    bool EnableCoreDumps = true;

    std::optional<double> CpuWeight;
    std::optional<i64> ThreadLimit;

    std::optional<std::string> NetworkInterface;
    std::optional<std::string> HostName;
    std::vector<NYT::NNet::TIP6Address> IPAddresses;
    bool EnableNat64 = false;
    bool DisableNetwork = false;

    THashMap<std::string, std::string> Labels;
    THashMap<std::string, std::string> Env;
    std::vector<std::string> CGroupControllers;
    std::vector<TDevice> Devices;
    std::optional<TRootFS> RootFS;

    //! Allowed places for creation of volumes and layers. "***" means any place.
    std::vector<std::string> Places;
};

////////////////////////////////////////////////////////////////////////////////

struct IPortoExecutor
    : public TRefCounted
{
    virtual void OnDynamicConfigChanged(const TPortoExecutorDynamicConfigPtr& newConfig) = 0;

    virtual TFuture<void> CreateContainer(const std::string& container) = 0;

    virtual TFuture<void> CreateContainer(const TRunnableContainerSpec& containerSpec, bool start) = 0;

    virtual TFuture<void> SetContainerProperty(
        const std::string& container,
        const std::string& property,
        const std::string& value) = 0;

    virtual TFuture<std::optional<std::string>> GetContainerProperty(
        const std::string& container,
        const std::string& property) = 0;

    virtual TFuture<THashMap<std::string, TErrorOr<std::string>>> GetContainerProperties(
        const std::string& container,
        const std::vector<std::string>& properties) = 0;
    virtual TFuture<THashMap<std::string, THashMap<std::string, TErrorOr<std::string>>>> GetContainerProperties(
        const std::vector<std::string>& containers,
        const std::vector<std::string>& properties) = 0;

    virtual TFuture<THashMap<std::string, i64>> GetContainerMetrics(
        const std::vector<std::string>& containers,
        const std::string& metric) = 0;
    virtual TFuture<void> DestroyContainer(const std::string& container) = 0;
    virtual TFuture<void> StopContainer(const std::string& container) = 0;
    virtual TFuture<void> StartContainer(const std::string& container) = 0;
    virtual TFuture<void> KillContainer(const std::string& container, int signal) = 0;
    virtual TFuture<void> RespawnContainer(const std::string& container) = 0;

    virtual TFuture<std::string> ConvertPath(const std::string& path, const std::string& container) = 0;

    // Returns absolute names of immediate children only.
    virtual TFuture<std::vector<std::string>> ListSubcontainers(
        const std::string& rootContainer,
        bool includeRoot) = 0;
    // Starts polling a given container, returns future with exit code of finished process.
    virtual TFuture<int> PollContainer(const std::string& container) = 0;

    // Returns future with exit code of finished process.
    // NB: Temporarily broken, see https://st.yandex-team.ru/PORTO-846 for details.
    virtual TFuture<int> WaitContainer(const std::string& container) = 0;

    virtual TFuture<std::string> CreateVolume(
        const std::string& path,
        const THashMap<std::string, std::string>& properties) = 0;
    virtual TFuture<void> LinkVolume(
        const std::string& path,
        const std::string& name,
        const std::string& target) = 0;
    virtual TFuture<void> UnlinkVolume(
        const std::string& path,
        const std::string& name,
        const std::string& target = AnyTarget) = 0;
    virtual TFuture<std::vector<std::string>> ListVolumePaths() = 0;
    virtual TFuture<std::vector<TVolumeSpec>> GetVolumes() = 0;

    virtual TFuture<void> ImportLayer(
        const std::string& archivePath,
        const std::string& layerId,
        const std::string& place,
        const std::string& container) = 0;
    virtual TFuture<void> RemoveLayer(
        const std::string& layerId,
        const std::string& place,
        bool async) = 0;
    virtual TFuture<std::vector<std::string>> ListLayers(const std::string& place) = 0;

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
