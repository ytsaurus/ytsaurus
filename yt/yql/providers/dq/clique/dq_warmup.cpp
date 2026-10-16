#include <yt/yql/providers/dq/common/yql_dq_warmup.h>
#include "dq_clique_warmup_session.h"

#include <contrib/ydb/library/yql/providers/dq/common/yql_dq_common.h>

#include <contrib/ydb/library/yql/providers/dq/api/grpc/api.grpc.pb.h>

#include <yql/essentials/utils/log/log.h>

#include <contrib/ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>

#include <library/cpp/svnversion/svnversion.h>

#include <util/string/builder.h>
#include <util/system/file.h>

namespace NYql {

using TFileResource = Yql::DqsProto::TFile;

namespace {

class TDqGrpcIsReadyClient {
public:
    explicit TDqGrpcIsReadyClient(int threads)
        : GrpcClient_(threads)
    {
    }

    bool CallIsReady(
        const TString& host,
        int port,
        const TVector<TFileResource>& files,
        bool udfOnly) const
    {
        NYdbGrpc::TGRpcClientConfig grpcConf(TStringBuilder() << host << ":" << port);
        auto service = GrpcClient_.CreateGRpcServiceConnection<Yql::DqsProto::DqService>(grpcConf);

        Yql::DqsProto::IsReadyRequest request;
        for (const auto& file : files) {
            if (udfOnly && file.GetObjectType() != Yql::DqsProto::TFile::EUDF_FILE) {
                continue;
            }
            *request.AddFiles() = file;
        }

        auto promise = NThreading::NewPromise<bool>();
        auto callback = [promise](NYdbGrpc::TGrpcStatus&& status, Yql::DqsProto::IsReadyResponse&& resp) mutable {
            promise.SetValue(status.Ok() && resp.GetIsReady());
        };

        NYdbGrpc::TCallMeta meta;
        meta.Timeout = std::chrono::seconds(2);
        service->DoRequest<Yql::DqsProto::IsReadyRequest, Yql::DqsProto::IsReadyResponse>(
            request, callback, &Yql::DqsProto::DqService::Stub::AsyncIsReady, meta);

        try {
            return promise.GetFuture().GetValueSync();
        } catch (...) {
            YQL_CLOG(TRACE, ProviderDq) << "IsReady failed: " << CurrentExceptionMessage();
            return false;
        }
    }

private:
    mutable NYdbGrpc::TGRpcClientLow GrpcClient_;
};

class TDqWarmupControl: public IDqWarmupControl {
public:
    TDqWarmupControl(
        TString dqGrpcHost,
        ui32 dqGrpcPort,
        TVector<TFileResource> files,
        bool enableCliqueWarmup,
        TVector<TResourceManagerOptions> ytBackends)
        : DqGrpcHost_(std::move(dqGrpcHost))
        , DqGrpcPort_(dqGrpcPort)
        , Files_(std::move(files))
        , GrpcClient_(2)
    {
        if (enableCliqueWarmup) {
            CliqueWarmupSession_ = CreateDqCliqueWarmupSession(
                [this] (
                    const TString& host,
                    int port,
                    const TVector<TFileResource>& files,
                    bool udfOnly) {
                    return GrpcClient_.CallIsReady(host, port, files, udfOnly);
                },
                std::move(ytBackends));
        }
    }

    void Stop() override {
        if (CliqueWarmupSession_) {
            CliqueWarmupSession_->Stop();
            CliqueWarmupSession_ = nullptr;
        }
    }

    bool IsReady(const TMap<TString, TString>& additionalUdfs) override {
        auto files = Files_;
        AppendAdditionalUdfs(additionalUdfs, &files);

        const bool dqReady = GrpcClient_.CallIsReady(
            DqGrpcHost_, DqGrpcPort_, files, /*udfOnly=*/false);
        if (!CliqueWarmupSession_) {
            return dqReady;
        }

        // Always call clique warmup even when dqReady is false: WarmupCliques
        // triggers artifact upload on clique nodes and may stay false for a while
        // before succeeding on a later poll.
        const bool cliquesReady = CliqueWarmupSession_->WarmupCliques(files);
        return dqReady && cliquesReady;
    }

private:
    void AppendAdditionalUdfs(const TMap<TString, TString>& additionalUdfs, TVector<TFileResource>* files) const {
        for (const auto& [path, objectId] : additionalUdfs) {
            TFileResource resource;
            resource.SetLocalPath(path);
            resource.SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
            resource.SetObjectId(objectId);
            resource.SetSize(TFile(path, OpenExisting | RdOnly).GetLength());
            files->push_back(std::move(resource));
        }
    }

private:
    const TString DqGrpcHost_;
    const ui32 DqGrpcPort_;
    const TVector<TFileResource> Files_;
    const TDqGrpcIsReadyClient GrpcClient_;
    IDqCliqueWarmupSessionPtr CliqueWarmupSession_;
};

class TDqWarmupControlFactory: public IDqWarmupControlFactory {
public:
    TDqWarmupControlFactory(
        TString dqGrpcHost,
        ui32 dqGrpcPort,
        bool enableStrip,
        bool enableCliqueWarmup,
        const TMap<TString, TString>& udfs,
        const TString& vanillaLitePath,
        const TString& vanillaLiteMd5,
        const THashSet<TString>& indexedUdfFilter,
        const TFileStoragePtr& fileStorage,
        TVector<TResourceManagerOptions> ytBackends)
        : DqGrpcHost_(std::move(dqGrpcHost))
        , DqGrpcPort_(dqGrpcPort)
        , EnableStrip_(enableStrip)
        , EnableCliqueWarmup_(enableCliqueWarmup)
        , IndexedUdfFilter_(indexedUdfFilter)
        , FileStorage_(fileStorage)
        , YtBackends_(std::move(ytBackends))
    {
        if (!vanillaLitePath.empty()) {
            TString path;
            TString objectId;
            std::tie(path, objectId) = GetPathAndObjectId(vanillaLitePath, GetProgramCommitId(), vanillaLiteMd5);

            TFileResource vanillaLite;
            vanillaLite.SetLocalPath(path);
            vanillaLite.SetName(vanillaLitePath.substr(vanillaLitePath.rfind('/') + 1));
            vanillaLite.SetObjectType(Yql::DqsProto::TFile::EEXE_FILE);
            vanillaLite.SetObjectId(objectId);
            vanillaLite.SetSize(TFile(path, OpenExisting | RdOnly).GetLength());
            Files_.push_back(std::move(vanillaLite));
        }

        for (const auto& [path, objectId] : udfs) {
            TString newPath;
            TString newObjectId;
            std::tie(newPath, newObjectId) = GetPathAndObjectId(path, objectId, objectId);

            TFileResource resource;
            resource.SetLocalPath(newPath);
            resource.SetObjectType(Yql::DqsProto::TFile::EUDF_FILE);
            resource.SetObjectId(newObjectId);
            resource.SetSize(TFile(newPath, OpenExisting | RdOnly).GetLength());
            Files_.push_back(std::move(resource));
        }
    }

    IDqWarmupControlPtr GetControl() override {
        return new TDqWarmupControl(
            DqGrpcHost_,
            DqGrpcPort_,
            Files_,
            EnableCliqueWarmup_,
            YtBackends_);
    }

    const THashSet<TString>& GetIndexedUdfFilter() override {
        return IndexedUdfFilter_;
    }

    bool StripEnabled() const override {
        return EnableStrip_;
    }

private:
    std::tuple<TString, TString> GetPathAndObjectId(
        const TString& path,
        const TString& objectId,
        const TString& md5)
    {
        if (!EnableStrip_) {
            return std::make_tuple(path, objectId);
        }

        TFileLinkPtr& fileLink = FileLinks_[objectId];
        if (!fileLink) {
            fileLink = FileStorage_->PutFileStripped(path, md5);
        }

        return std::make_tuple(fileLink->GetPath(), objectId + DqStrippedSuffied());
    }

private:
    const TString DqGrpcHost_;
    const ui32 DqGrpcPort_;
    const bool EnableStrip_;
    const bool EnableCliqueWarmup_;
    TVector<TFileResource> Files_;
    const THashSet<TString> IndexedUdfFilter_;
    THashMap<TString, TFileLinkPtr> FileLinks_;
    const TFileStoragePtr FileStorage_;
    const TVector<TResourceManagerOptions> YtBackends_;
};

} // namespace

IDqWarmupControlFactoryPtr CreateDqWarmupControlFactory(
    const NProto::TDqConfig& config,
    const TMap<TString, TString>& udfs,
    const TFileStoragePtr& fileStorage)
{
    const bool enableCliqueWarmup = config.GetControl().GetEnableCliqueWarmup();
    THashSet<TString> indexedUdfFilter(
        config.GetControl().GetIndexedUdfsToWarmup().begin(),
        config.GetControl().GetIndexedUdfsToWarmup().end());

    TVector<TResourceManagerOptions> ytBackends;
    if (enableCliqueWarmup) {
        ytBackends = BuildWarmupYtBackendsFromDqConfig(config);
    }

    return CreateDqWarmupControlFactory(
        config.HasHost() ? config.GetHost() : TString("localhost"),
        config.GetPort(),
        config.GetYtBackends().empty() ? TString() : config.GetYtBackends(0).GetVanillaJobLite(),
        config.GetYtBackends().empty() ? TString() : config.GetYtBackends(0).GetVanillaJobLiteMd5(),
        config.GetControl().GetEnableStrip(),
        enableCliqueWarmup,
        indexedUdfFilter,
        udfs,
        fileStorage,
        std::move(ytBackends));
}

IDqWarmupControlFactoryPtr CreateDqWarmupControlFactory(
    const TString& dqGrpcHost,
    const ui32 dqGrpcPort,
    const TString& vanillaJobLite,
    const TString& vanillaJobLiteMd5,
    const bool enableStrip,
    const bool enableCliqueWarmup,
    const THashSet<TString>& indexedUdfFilter,
    const TMap<TString, TString>& udfs,
    const TFileStoragePtr& fileStorage,
    const TVector<TResourceManagerOptions>& ytBackends)
{
    return new TDqWarmupControlFactory(
        dqGrpcHost,
        dqGrpcPort,
        enableStrip,
        enableCliqueWarmup,
        udfs,
        vanillaJobLite,
        vanillaJobLiteMd5,
        indexedUdfFilter,
        fileStorage,
        ytBackends);
}

} // namespace NYql
