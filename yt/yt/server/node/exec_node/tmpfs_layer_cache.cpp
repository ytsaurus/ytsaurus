#include "tmpfs_layer_cache.h"

#include "bootstrap.h"
#include "helpers.h"
#include "layer_location.h"
#include "private.h"

#include <yt/yt/server/node/cluster_node/config.h>
#include <yt/yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/yt/server/node/data_node/config.h>

#include <yt/yt/library/containers/porto_executor.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/proc.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <util/system/fs.h>

namespace NYT::NExecNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NClusterNode;
using namespace NContainers;
using namespace NDataNode;
using namespace NObjectClient;
using namespace NYTree;
using namespace NYson;
using namespace NFS;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = ExecNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TTmpfsLayerCache::TTmpfsLayerCache(
    IBootstrap* const bootstrap,
    TTmpfsLayerCacheConfigPtr config,
    TClusterNodeDynamicConfigManagerPtr dynamicConfigManager,
    IInvokerPtr controlInvoker,
    IMemoryUsageTrackerPtr memoryUsageTracker,
    const TString& cacheName,
    IPortoExecutorPtr portoExecutor,
    TAbsorbLayerCallback absorbLayer)
    : Config_(std::move(config))
    , DynamicConfigManager_(std::move(dynamicConfigManager))
    , ControlInvoker_(std::move(controlInvoker))
    , MemoryUsageTracker_(std::move(memoryUsageTracker))
    , CacheName_(cacheName)
    , Bootstrap_(bootstrap)
    , PortoExecutor_(std::move(portoExecutor))
    , AbsorbLayer_(std::move(absorbLayer))
    , Profiler_(ExecNodeProfiler()
        .WithPrefix("/layer_cache")
        .WithTag("cache_name", CacheName_))
    , HitCounter_(Profiler_.Counter("/tmpfs_cache_hits"))
    , UpdateFailedCounter_(Profiler_.Gauge("/update_failed"))
    , TmpfsLayerCacheCounters_(Profiler_.WithGlobal())
{
    if (Bootstrap_) {
        Bootstrap_->SubscribePopulateAlerts(BIND(
            &TTmpfsLayerCache::PopulateTmpfsAlert,
            MakeWeak(this)));
    }
}

TLayerPtr TTmpfsLayerCache::FindLayer(const TArtifactKey& artifactKey)
{
    auto guard = Guard(DataSpinLock_);
    auto it = CachedLayers_.find(artifactKey);
    if (it != CachedLayers_.end()) {
        auto layer = it->second;
        guard.Release();

        // The following counter can help find layers that do not benefit from residing in tmpfs layer cache.
        auto cacheHitsCypressPathCounter = TmpfsLayerCacheCounters_.GetCounter(
            TTagSet({{"cypress_path", artifactKey.data_source().path()}}),
            "/tmpfs_layer_hits");

        cacheHitsCypressPathCounter.Increment();
        HitCounter_.Increment();
        return layer;
    } else {
        // The following counter can help find layers that could benefit from residing in tmpfs layer cache.
        auto cacheMissesCypressPathCounter = TmpfsLayerCacheCounters_.GetCounter(
            TTagSet({{"cypress_path", artifactKey.data_source().path()}}),
            "/tmpfs_layer_misses");

        cacheMissesCypressPathCounter.Increment();
    }
    return nullptr;
}

TFuture<void> TTmpfsLayerCache::Initialize()
{
    if (!Config_->LayersDirectoryPath) {
        return OKFuture;
    }

    auto path = CombinePaths(NFs::CurrentWorkingDirectory(), Format("%v_tmpfs_layers", CacheName_));

    {
        YT_LOG_DEBUG("Cleanup tmpfs layer cache volume (Path: %v)", path);
        auto error = WaitFor(PortoExecutor_->UnlinkVolume(TString(path), "self"));
        if (!error.IsOK()) {
            YT_LOG_DEBUG(error, "Failed to unlink volume (Path: %v)", path);
        }
    }

    TFuture<void> result;
    try {
        YT_LOG_DEBUG("Create tmpfs layer cache volume (Path: %v)", path);

        MakeDirRecursive(path, 0777);

        THashMap<TString, TString> volumeProperties;
        volumeProperties["backend"] = "tmpfs";
        volumeProperties["permissions"] = "0777";
        volumeProperties["space_limit"] = ToString(Config_->Capacity);

        WaitFor(PortoExecutor_->CreateVolume(TString(path), volumeProperties))
            .ThrowOnError();

        MemoryUsageTracker_->Acquire(Config_->Capacity);

        auto locationConfig = New<NDataNode::TLayerLocationConfig>();
        locationConfig->Quota = Config_->Capacity;
        locationConfig->LowWatermark = 0;
        locationConfig->MinDiskSpace = 0;
        locationConfig->Path = path;
        locationConfig->LocationIsAbsolute = false;
        locationConfig->ResidesOnTmpfs = true;

        TmpfsLocation_ = New<TLayerLocation>(
            std::move(locationConfig),
            DynamicConfigManager_,
            nullptr,
            PortoExecutor_,
            PortoExecutor_,
            Format("%v_tmpfs_layer", CacheName_));

        WaitFor(TmpfsLocation_->Initialize())
            .ThrowOnError();

        if (Bootstrap_) {
            LayerUpdateExecutor_ = New<TPeriodicExecutor>(
                ControlInvoker_,
                BIND(&TTmpfsLayerCache::UpdateLayers, MakeWeak(this)),
                Config_->LayersUpdatePeriod);

            LayerUpdateExecutor_->Start();
        }

        result = Initialized_.ToFuture();
    } catch (const std::exception& ex) {
        auto error = TError("Failed to create %v tmpfs layer volume cache", CacheName_) << ex;
        SetAlert(error);
        // That's a fatal error; tmpfs layer cache is broken and we shouldn't start jobs on this node.
        result = MakeFuture(error);
    }
    return result;
}

TFuture<void> TTmpfsLayerCache::Disable(const TError& error, bool persistentDisable)
{
    YT_ASSERT_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_WARNING("Disable tmpfs layer cache (Path: %v)", CacheName_);

    if (TmpfsLocation_) {
        MemoryUsageTracker_->Release(Config_->Capacity);

        if (LayerUpdateExecutor_) {
            return LayerUpdateExecutor_->Stop()
                .Apply(BIND(&TLayerLocation::Disable, TmpfsLocation_, error, persistentDisable));
        } else {
            TmpfsLocation_->Disable(error, persistentDisable);
            return OKFuture;
        }
    } else {
        return OKFuture;
    }
}

void TTmpfsLayerCache::BuildOrchid(TFluentMap fluent) const
{
    auto guard1 = Guard(DataSpinLock_);
    auto guard2 = Guard(AlertSpinLock_);
    fluent
        .Item("layer_count").Value(CachedLayers_.size())
        .Item("alert").Value(Alert_)
        .Item("layers").BeginMap()
            .DoFor(CachedLayers_, [] (TFluentMap fluent, const auto& item) {
                fluent
                    .Item(item.second->GetCypressPath())
                        .BeginMap()
                            .Item("size").Value(item.second->GetSize())
                            .Item("hit_count").Value(item.second->GetHitCount())
                        .EndMap();
            })
        .EndMap();
}

const TLayerLocationPtr& TTmpfsLayerCache::GetLocation() const
{
    return TmpfsLocation_;
}

void TTmpfsLayerCache::PopulateTmpfsAlert(std::vector<TError>* errors)
{
    auto guard = Guard(AlertSpinLock_);
    if (!Alert_.IsOK()) {
        errors->push_back(Alert_);
    }
}

void TTmpfsLayerCache::SetAlert(const TError& error)
{
    auto guard = Guard(AlertSpinLock_);
    if (error.IsOK() && !Alert_.IsOK()) {
        YT_LOG_INFO("Tmpfs layer cache alert reset (CacheName: %v)", CacheName_);
        UpdateFailedCounter_.Update(0);
    } else if (!error.IsOK()) {
        YT_LOG_WARNING(error, "Tmpfs layer cache alert set (CacheName: %v)", CacheName_);
        UpdateFailedCounter_.Update(1);
    }

    Alert_ = error;
}

void TTmpfsLayerCache::UpdateLayers()
{
    const auto& client = Bootstrap_->GetClient();

    auto tag = TGuid::Create();
    auto Logger = ExecNodeLogger()
        .WithTag("Tag: %v", tag);

    YT_LOG_INFO("Started updating tmpfs layers");

    TListNodeOptions listNodeOptions;
    listNodeOptions.ReadFrom = EMasterChannelKind::Cache;
    listNodeOptions.Attributes = {"id"};
    auto listNodeRspOrError = WaitFor(client->ListNode(
        *Config_->LayersDirectoryPath,
        listNodeOptions));

    if (!listNodeRspOrError.IsOK()) {
        SetAlert(TError(
            NExecNode::EErrorCode::TmpfsLayerImportFailed,
            "Failed to list %v tmpfs layers directory %v",
            CacheName_,
            Config_->LayersDirectoryPath)
            << listNodeRspOrError);
        return;
    }
    const auto& listNodeRsp = listNodeRspOrError.Value();

    THashSet<NYPath::TYPath> paths;
    try {
        auto listNode = ConvertToNode(listNodeRsp)->AsList();
        for (const auto& node : listNode->GetChildren()) {
            auto idString = node->Attributes().Get<TString>("id");
            auto id = TObjectId::FromString(idString);
            paths.insert(FromObjectId(id));
        }
    } catch (const std::exception& ex) {
        SetAlert(TError(
            NExecNode::EErrorCode::TmpfsLayerImportFailed,
            "Tmpfs layers directory %v has invalid structure",
            Config_->LayersDirectoryPath)
            << ex);
        return;
    }

    YT_LOG_INFO(
        "Listed tmpfs layers (CacheName: %v, Count: %v)",
        CacheName_,
        paths.size());

    {
        THashMap<NYPath::TYPath, TFetchedArtifactKey> cachedLayerDescriptors;
        for (const auto& path : paths) {
            auto it = CachedLayerDescriptors_.find(path);
            if (it != CachedLayerDescriptors_.end()) {
                cachedLayerDescriptors.insert(*it);
            } else {
                cachedLayerDescriptors.emplace(path, TFetchedArtifactKey{});
            }
        }

        CachedLayerDescriptors_.swap(cachedLayerDescriptors);
    }

    std::vector<TFuture<TFetchedArtifactKey>> futures;
    for (const auto& pair : CachedLayerDescriptors_) {
        auto future = BIND(
            [=, this, this_ = MakeStrong(this)] {
                const auto& path = pair.first;
                const auto& fetchedKey = pair.second;
                auto revision = fetchedKey.ArtifactKey
                    ? fetchedKey.ContentRevision
                    : NHydra::NullRevision;
                return FetchLayerArtifactKeyIfRevisionChanged(path, revision, Bootstrap_, Logger);
            })
            .AsyncVia(GetCurrentInvoker())
            .Run();

        futures.push_back(std::move(future));
    }

    auto fetchResultsOrError = WaitFor(AllSucceeded(futures));
    if (!fetchResultsOrError.IsOK()) {
        SetAlert(TError(NExecNode::EErrorCode::TmpfsLayerImportFailed, "Failed to fetch tmpfs layer descriptions")
            << fetchResultsOrError);
        return;
    }

    int index = 0;
    THashSet<TArtifactKey> newArtifacts;
    for (auto& [_, fetchedKey] : CachedLayerDescriptors_) {
        const auto& fetchResult = fetchResultsOrError.Value()[index];
        if (fetchResult.ArtifactKey) {
            fetchedKey = fetchResult;
        }
        ++index;
        YT_VERIFY(fetchedKey.ArtifactKey);
        newArtifacts.insert(*fetchedKey.ArtifactKey);
    }

    YT_LOG_DEBUG("Listed unique tmpfs layers (Count: %v)", newArtifacts.size());

    {
        std::vector<TArtifactKey> artifactsToRemove;
        artifactsToRemove.reserve(CachedLayers_.size());

        auto guard = Guard(DataSpinLock_);
        for (const auto& [key, layer] : CachedLayers_) {
            if (!newArtifacts.contains(key)) {
                artifactsToRemove.push_back(key);
            } else {
                newArtifacts.erase(key);
            }
        }

        for (const auto& artifactKey : artifactsToRemove) {
            CachedLayers_.erase(artifactKey);
        }

        guard.Release();

        YT_LOG_INFO_IF(
            !artifactsToRemove.empty(),
            "Released cached tmpfs layers (Count: %v)",
            artifactsToRemove.size());
    }

    std::vector<TFuture<TLayerPtr>> newLayerFutures;
    newLayerFutures.reserve(newArtifacts.size());

    TArtifactDownloadOptions downloadOptions{
        .WorkloadDescriptorAnnotations = {"Type: TmpfsLayersUpdate"},
    };
    for (const auto& artifactKey : newArtifacts) {
        newLayerFutures.push_back(AbsorbLayer_(
            artifactKey,
            downloadOptions,
            tag,
            TmpfsLocation_));
    }

    auto newLayersOrError = WaitFor(AllSet(newLayerFutures));
    if (!newLayersOrError.IsOK()) {
        SetAlert(TError(NExecNode::EErrorCode::TmpfsLayerImportFailed, "Failed to import new tmpfs layers")
            << newLayersOrError);
        return;
    }

    bool hasFailedLayer = false;
    bool hasImportedLayer = false;
    for (const auto& newLayerOrError : newLayersOrError.Value()) {
        if (!newLayerOrError.IsOK()) {
            hasFailedLayer = true;
            SetAlert(TError(NExecNode::EErrorCode::TmpfsLayerImportFailed, "Failed to import new %v tmpfs layer", CacheName_)
                << newLayerOrError);
            continue;
        }

        const auto& layer = newLayerOrError.Value();
        YT_LOG_INFO(
            "Successfully imported new tmpfs layer (LayerId: %v, ArtifactPath: %v, CacheName: %v)",
            layer->GetMeta().Id,
            layer->GetMeta().artifact_key().data_source().path(),
            CacheName_);
        hasImportedLayer = true;

        TArtifactKey key;
        key.CopyFrom(layer->GetMeta().artifact_key());

        auto guard = Guard(DataSpinLock_);
        CachedLayers_[key] = layer;
    }

    if (!hasFailedLayer) {
        // No alert, everything is fine.
        SetAlert(TError());
    }

    if (hasImportedLayer || !hasFailedLayer) {
        // If at least one tmpfs layer was successfully imported,
        // we consider tmpfs layer cache initialization completed.
        Initialized_.TrySet();
    }

    YT_LOG_INFO("Finished updating tmpfs layers");
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_REFCOUNTED_TYPE(TTmpfsLayerCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
