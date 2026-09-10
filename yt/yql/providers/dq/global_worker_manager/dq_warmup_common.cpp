#include "dq_warmup.h"

#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/threading/future/future.h>

namespace NYql {

void UploadWarmupArtifactsToYt(
    NActors::TActorSystem* actorSystem,
    const TIntrusivePtr<ICoordinationHelper>& coordinator,
    const TVector<TResourceManagerOptions>& ytBackends,
    const TString& vanillaJobLite,
    const TMap<TString, TString>& udfsWithMd5)
{
    const TString objectId = GetProgramCommitId();
    TVector<NThreading::TFuture<void>> uploadFutures;

    TVector<TResourceFile> udfFiles;
    for (const auto& [path, md5] : udfsWithMd5) {
        TResourceFile file(path);
        file.ObjectId = md5;
        file.RemoteFileName = md5;
        file.Attributes["file_name"] = md5;
        udfFiles.push_back(std::move(file));
    }

    TVector<TResourceFile> exeFiles;
    if (!vanillaJobLite.empty()) {
        TResourceFile exeFile(vanillaJobLite);
        exeFile.ObjectId = objectId;
        exeFile.RemoteFileName = vanillaJobLite.substr(vanillaJobLite.rfind('/') + 1);
        exeFile.Attributes["file_name"] = exeFile.GetRemoteFileName();
        exeFiles.push_back(std::move(exeFile));
    }

    for (const auto& backendOptions : ytBackends) {
        const auto& ytBackend = backendOptions.YtBackend;

        if (!udfFiles.empty()) {
            auto promise = NThreading::NewPromise<void>();
            uploadFutures.push_back(promise.GetFuture());

            TResourceManagerOptions opts;
            opts.YtBackend = ytBackend;
            opts.Files = udfFiles;
            opts.UploadPrefix = ytBackend.GetUploadPrefix() + "/udfs";
            opts.Uploaded = promise;
            actorSystem->Register(CreateResourceUploader(opts, coordinator));
        }

        if (!exeFiles.empty()) {
            auto promise = NThreading::NewPromise<void>();
            uploadFutures.push_back(promise.GetFuture());

            TResourceManagerOptions opts;
            opts.YtBackend = ytBackend;
            opts.Files = exeFiles;
            opts.UploadPrefix = ytBackend.GetUploadPrefix() + "/bin/" + objectId;
            opts.LockName = objectId + "." + ytBackend.GetClusterName();
            opts.Uploaded = promise;
            actorSystem->Register(CreateResourceUploader(opts, coordinator));
        }
    }

    if (!uploadFutures.empty()) {
        NThreading::WaitAll(uploadFutures).GetValueSync();
    }
}

} // namespace NYql
