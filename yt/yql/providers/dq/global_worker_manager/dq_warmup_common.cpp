#include "dq_warmup.h"

#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_common.h>

#include <yt/yql/providers/dq/global_worker_manager/coordination_helper.h>

#include <yql/essentials/utils/log/log.h>

#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/threading/future/future.h>

namespace NYql {

void UploadWarmupArtifactsToYt(
    NActors::TActorSystem* actorSystem,
    const TIntrusivePtr<ICoordinationHelper>& coordinator,
    const TVector<TResourceManagerOptions>& ytBackends,
    const TString& vanillaJobLite,
    const TString& vanillaJobLiteMd5,
    const TMap<TString, TString>& udfsWithMd5,
    bool enableStrip,
    const TFileStoragePtr& fileStorage)
{
    const TString suffix = enableStrip ? DqStrippedSuffied() : TString{};
    const TString objectId = GetProgramCommitId() + suffix;
    TVector<NThreading::TFuture<void>> uploadFutures;

    TVector<TResourceFile> udfFiles;
    for (const auto& [path, md5] : udfsWithMd5) {
        const auto fileLink = enableStrip ? fileStorage->PutFileStripped(path, md5) : TFileLinkPtr{};
        const auto& uploadPath = fileLink ? fileLink->GetPath().GetPath() : path;
        const TString udfObjectId = md5 + suffix;
        TResourceFile file(uploadPath);
        file.ObjectId = udfObjectId;
        file.RemoteFileName = udfObjectId;
        file.Attributes["file_name"] = udfObjectId;
        udfFiles.push_back(std::move(file));
    }

    TVector<TResourceFile> exeFiles;
    if (!vanillaJobLite.empty()) {
        const auto fileLink = enableStrip
            ? fileStorage->PutFileStripped(vanillaJobLite, vanillaJobLiteMd5)
            : TFileLinkPtr{};
        const auto& uploadPath = fileLink ? fileLink->GetPath().GetPath() : vanillaJobLite;
        TResourceFile exeFile(uploadPath);
        exeFile.ObjectId = objectId;
        exeFile.RemoteFileName = "dq_vanilla_job.lite";
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
        const auto uploadTimeout = TDuration::Minutes(30);
        auto allUploads = NThreading::WaitAll(uploadFutures);
        if (!allUploads.Wait(uploadTimeout)) {
            YQL_CLOG(ERROR, ProviderDq)
                << "Timed out waiting for " << uploadFutures.size()
                << " DQ warmup artifact uploads after " << uploadTimeout;
            return;
        }
        allUploads.GetValueSync();
    }
}

} // namespace NYql
