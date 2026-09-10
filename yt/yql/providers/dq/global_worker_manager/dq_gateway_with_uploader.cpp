#include "dq_gateway_with_uploader.h"

#include <yt/yql/providers/dq/actors/yt/resource_manager.h>

#include <contrib/ydb/library/yql/providers/dq/api/protos/service.pb.h>
#include <contrib/ydb/library/yql/providers/dq/planner/execution_planner.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/system/file.h>
#include <util/system/mutex.h>

#include <memory>

namespace NYql {

namespace {

struct TCollectedFiles {
    // UDF and user files: uploaded to <UploadPrefix>/udfs/
    TVector<TResourceFile> UdfFiles;
    // Exe files: uploaded to <UploadPrefix>/bin/<objectId>/
    TVector<TResourceFile> ExeFiles;
};

struct TExeUploadCacheEntry {
    NThreading::TFuture<void> Future;
    TInstant CompletedAt;
    ui64 Generation = 0;
};

struct TExeUploadCache {
    TMutex Mutex;
    THashMap<TString, TExeUploadCacheEntry> Entries;
    ui64 NextGeneration = 0;
};

// Collect all unique files from task metas in the plan, split by type.
TCollectedFiles CollectFilesFromPlan(const NDqs::TPlan& plan) {
    THashMap<TString, TResourceFile> udfById;
    THashMap<TString, TResourceFile> exeById;

    for (const auto& task : plan.Tasks) {
        Yql::DqsProto::TTaskMeta taskMeta;
        if (!task.GetMeta().UnpackTo(&taskMeta)) {
            YQL_CLOG(WARN, ProviderDq) << "TDqGatewayWithUploader: failed to unpack TTaskMeta"
                << " type_url=" << task.GetMeta().GetTypeName();
            continue;
        }

        for (const auto& file : taskMeta.GetFiles()) {
            const TString& objectId = file.GetObjectId();
            // Skip files without objectId or local path (exe files may have no local path)
            if (objectId.empty()) {
                continue;
            }

            const bool isExe = file.GetObjectType() == Yql::DqsProto::TFile::EEXE_FILE;
            auto& byId = isExe ? exeById : udfById;
            if (byId.contains(objectId) || file.GetLocalPath().empty()) {
                continue;
            }
            TResourceFile resourceFile;
            resourceFile.ObjectId = objectId;
            resourceFile.LocalFileName = file.GetName();
            // COMPAT(uzhas): exe files use the file name as remote name, UDFs the objectId (same as GWM logic).
            resourceFile.RemoteFileName = isExe ? file.GetName() : objectId;
            resourceFile.Attributes["file_name"] = resourceFile.GetRemoteFileName();
            resourceFile.File = ::TFile(file.GetLocalPath(), RdOnly | OpenExisting);
            byId.emplace(objectId, std::move(resourceFile));
        }
    }

    TCollectedFiles result;
    result.UdfFiles.reserve(udfById.size());
    for (auto&& [_, rf] : udfById) {
        result.UdfFiles.push_back(std::move(rf));
    }
    result.ExeFiles.reserve(exeById.size());
    for (auto&& [_, rf] : exeById) {
        result.ExeFiles.push_back(std::move(rf));
    }
    return result;
}

// Remote GWM CheckFiles rejects client-side LocalPath; keep objectId/name only.
void ClearLocalPathsFromPlan(NDqs::TPlan& plan) {
    for (auto& task : plan.Tasks) {
        Yql::DqsProto::TTaskMeta taskMeta;
        if (!task.GetMeta().UnpackTo(&taskMeta)) {
            YQL_CLOG(WARN, ProviderDq) << "TDqGatewayWithUploader: failed to unpack TTaskMeta"
                << " type_url=" << task.GetMeta().GetTypeName();
            continue;
        }
        bool changed = false;
        for (auto& file : *taskMeta.MutableFiles()) {
            if (!file.GetLocalPath().empty()) {
                file.ClearLocalPath();
                changed = true;
            }
        }
        if (changed) {
            task.MutableMeta()->PackFrom(taskMeta);
        }
    }
}

class TDqGatewayWithUploader : public IDqGateway {
public:
    TDqGatewayWithUploader(
        TIntrusivePtr<IDqGateway> underlying,
        NActors::TActorSystem* actorSystem,
        TResourceManagerOptions uploadOptions,
        TIntrusivePtr<ICoordinationHelper> coordinator,
        TUploadClusterResolver resolveUploadCluster)
        : Underlying_(std::move(underlying))
        , ActorSystem_(actorSystem)
        , UploadOptions_(std::move(uploadOptions))
        , Coordinator_(std::move(coordinator))
        , ResolveUploadCluster_(std::move(resolveUploadCluster))
    {
    }

    void Stop() override {
        Underlying_->Stop();
    }

    NThreading::TFuture<void> OpenSession(const TString& sessionId, const TString& username) override {
        return Underlying_->OpenSession(sessionId, username);
    }

    NThreading::TFuture<void> CloseSessionAsync(const TString& sessionId) override {
        return Underlying_->CloseSessionAsync(sessionId);
    }

    NThreading::TFuture<TResult> ExecutePlan(
        const TString& sessionId,
        NDqs::TPlan&& plan,
        const TVector<TString>& columns,
        const THashMap<TString, TString>& secureParams,
        const THashMap<TString, TString>& graphParams,
        const TDqSettings::TPtr& settings,
        const TDqProgressWriter& progressWriter,
        const THashMap<TString, TString>& modulesMapping,
        bool discard,
        ui64 executionTimeout) override
    {
        TCollectedFiles collected;
        try {
            collected = CollectFilesFromPlan(plan);
        } catch (...) {
            return NThreading::MakeErrorFuture<TResult>(std::current_exception());
        }

        TResourceManagerOptions uploadOpts = UploadOptions_;
        if (ResolveUploadCluster_) {
            ResolveUploadCluster_(&uploadOpts, settings);
        }

        TVector<NThreading::TFuture<void>> uploadFutures;

        // Upload UDF/user files to <UploadPrefix>/udfs/
        if (!collected.UdfFiles.empty()) {
            auto promise = NThreading::NewPromise<void>();
            uploadFutures.push_back(promise.GetFuture());

            TResourceManagerOptions opts = uploadOpts;
            opts.Files = collected.UdfFiles;
            opts.UploadPrefix = uploadOpts.UploadPrefix + "/udfs";
            opts.LockName.clear();
            opts.Uploaded = promise;

            ActorSystem_->Register(CreateResourceUploader(opts, Coordinator_));
        }

        // Upload exe files to <UploadPrefix>/bin/<objectId>/
        for (const auto& exeFile : collected.ExeFiles) {
            uploadFutures.push_back(UploadExeFile(uploadOpts, exeFile));
        }

        if (uploadFutures.empty()) {
            YQL_CLOG(DEBUG, ProviderDq) << "TDqGatewayWithUploader: no files to upload, forwarding ExecutePlan";
            ClearLocalPathsFromPlan(plan);
            return Underlying_->ExecutePlan(
                sessionId, std::move(plan), columns, secureParams, graphParams,
                settings, progressWriter, modulesMapping, discard, executionTimeout);
        }

        YQL_CLOG(DEBUG, ProviderDq) << "TDqGatewayWithUploader: uploading "
            << collected.UdfFiles.size() << " UDF file(s) and "
            << collected.ExeFiles.size() << " exe file(s) to "
            << uploadOpts.YtBackend.GetClusterName()
            << " before ExecutePlan";

        auto underlying = Underlying_;
        return NThreading::WaitAll(uploadFutures).Apply(
            [underlying, sessionId, plan = std::move(plan), columns, secureParams, graphParams,
             settings, progressWriter, modulesMapping, discard, executionTimeout]
            (const NThreading::TFuture<void>& f) mutable -> NThreading::TFuture<TResult>
            {
                try {
                    f.TryRethrow();
                } catch (...) {
                    YQL_CLOG(ERROR, ProviderDq) << "TDqGatewayWithUploader: file upload failed: "
                        << CurrentExceptionMessage();
                    TResult result;
                    result.SetStatus(TIssuesIds::DEFAULT_ERROR);
                    result.AddIssue(TIssue(TStringBuilder()
                        << "File upload failed: " << CurrentExceptionMessage()));
                    return NThreading::MakeFuture(result);
                }

                YQL_CLOG(DEBUG, ProviderDq) << "TDqGatewayWithUploader: all files uploaded, forwarding ExecutePlan";
                ClearLocalPathsFromPlan(plan);
                return underlying->ExecutePlan(
                    sessionId, std::move(plan), columns, secureParams, graphParams,
                    settings, progressWriter, modulesMapping, discard, executionTimeout);
            });
    }

    TString GetVanillaJobPath() override {
        return Underlying_->GetVanillaJobPath();
    }

    TString GetVanillaJobMd5() override {
        return Underlying_->GetVanillaJobMd5();
    }

private:
    NThreading::TFuture<void> UploadExeFile(
        const TResourceManagerOptions& uploadOptions,
        const TResourceFile& exeFile)
    {
        static constexpr TDuration CacheTtl = TDuration::Hours(1);

        const TString cluster = uploadOptions.YtBackend.GetClusterName();
        const TString uploadPrefix = uploadOptions.UploadPrefix + "/bin/" + exeFile.ObjectId;
        const TString cacheKey = TStringBuilder() << cluster << '\0' << uploadPrefix;
        const TInstant now = TInstant::Now();

        NThreading::TPromise<void> promise;
        NThreading::TFuture<void> future;
        ui64 generation = 0;
        with_lock (ExeUploadCache_->Mutex) {
            auto it = ExeUploadCache_->Entries.find(cacheKey);
            if (it != ExeUploadCache_->Entries.end()) {
                const auto& entry = it->second;
                if (!entry.Future.IsReady() ||
                    (entry.Future.HasValue() && now - entry.CompletedAt < CacheTtl))
                {
                    return entry.Future;
                }
                ExeUploadCache_->Entries.erase(it);
            }

            promise = NThreading::NewPromise<void>();
            future = promise.GetFuture();
            generation = ++ExeUploadCache_->NextGeneration;
            ExeUploadCache_->Entries.emplace(cacheKey, TExeUploadCacheEntry{
                .Future = future,
                .Generation = generation,
            });
        }

        future.NoexceptSubscribe(
            [cache = ExeUploadCache_, cacheKey, generation](const NThreading::TFuture<void>& completed) noexcept {
                with_lock (cache->Mutex) {
                    auto it = cache->Entries.find(cacheKey);
                    if (it == cache->Entries.end() || it->second.Generation != generation) {
                        return;
                    }
                    if (completed.HasValue()) {
                        it->second.CompletedAt = TInstant::Now();
                    } else {
                        cache->Entries.erase(it);
                    }
                }
            });

        TResourceManagerOptions opts = uploadOptions;
        opts.Files = {exeFile};
        opts.UploadPrefix = uploadPrefix;
        opts.LockName = exeFile.ObjectId + "." + cluster;
        opts.Uploaded = promise;
        ActorSystem_->Register(CreateResourceUploader(opts, Coordinator_));

        return future;
    }

    const TIntrusivePtr<IDqGateway> Underlying_;
    NActors::TActorSystem* const ActorSystem_;
    const TResourceManagerOptions UploadOptions_;
    const TIntrusivePtr<ICoordinationHelper> Coordinator_;
    const TUploadClusterResolver ResolveUploadCluster_;
    const std::shared_ptr<TExeUploadCache> ExeUploadCache_ = std::make_shared<TExeUploadCache>();
};

} // namespace

TIntrusivePtr<IDqGateway> CreateDqGatewayWithUploader(
    TIntrusivePtr<IDqGateway> underlying,
    NActors::TActorSystem* actorSystem,
    TResourceManagerOptions uploadOptions,
    TIntrusivePtr<ICoordinationHelper> coordinator,
    TUploadClusterResolver resolveUploadCluster)
{
    return new TDqGatewayWithUploader(
        std::move(underlying),
        actorSystem,
        std::move(uploadOptions),
        std::move(coordinator),
        std::move(resolveUploadCluster));
}

} // namespace NYql
