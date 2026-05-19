#include "fmr_download.h"
#include <yql/essentials/core/file_storage/http_download/http_download.h>
#include <yt/yql/providers/yt/fmr/vanilla/peer_tracker/yql_yt_vanilla_peer_tracker.h>

#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/client_method_options.h>

#include <util/generic/guid.h>

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/log_component.h>

namespace NYql {

namespace {

using namespace NFmr;

constexpr ui16 FMR_HTTP_MON_PORT = 8003;

class TFmrDownloader: public NYql::NFS::IDownloader {
public:
    explicit TFmrDownloader(const TFileStorageConfig& config)
        : Http_(MakeHttpDownloader(config))
    {
    }
    ~TFmrDownloader() = default;

    bool Accept(const THttpURL& url) final {
        const auto rawScheme = url.GetField(NUri::TField::FieldScheme);
        return NUri::EqualNoCase(rawScheme, "fmr");
    }

    std::tuple<NYql::NFS::TDataProvider, TString, TString> Download(const THttpURL& url, const TString& oauthToken, const TString& oldEtag, const TString& oldLastModified) final {
        TString ytCluster(url.GetField(NUri::TField::FieldHost));
        TFsPath path(url.GetField(NUri::TField::FieldPath));
        auto split = path.PathSplit();
        TString alias(split[0]);
        ui64 hostIndex = 0;
        if (split.size() > 1) {
            hostIndex = FromString(split[1]);
        }

        NYT::TCreateClientOptions clientOptions;
        if (oauthToken) {
            clientOptions.Token(oauthToken);
        }
        auto client = NYT::CreateClient(ytCluster, clientOptions);
        auto attrs = client->GetOperation("*" + alias);
        YQL_ENSURE(attrs.Id.Defined(), "GetOperation returned no ID for alias " << alias);
        TString operationId = GetGuidAsString(*attrs.Id);
        YQL_CLOG(DEBUG, FastMapReduce) << "Resolved alias " << alias << " to operation id " << operationId;

        TVanillaExternalPeerTrackerSettings settings;
        settings.Cluster = ytCluster;
        settings.OperationId = operationId;
        settings.Token = oauthToken;
        settings.WaitForPeers = true;
        TVanillaExternalPeerTracker tracker(settings);
        tracker.Start();
        auto host = tracker.GetPeerAddress(hostIndex);
        YQL_CLOG(DEBUG, FastMapReduce) << "Resolved host " << host << " with index " << hostIndex;
        if (!host) {
            throw yexception() << "No IP address is assigned yet, try later";
        }

        TFsPath rest("/");
        for (ui32 i = 1; i < split.size(); ++i) {
            rest /= TFsPath(split[i]);
        }

        THttpURL httpUrl(host, FMR_HTTP_MON_PORT, rest.GetPath());
        return Http_->Download(httpUrl, oauthToken, oldEtag, oldLastModified);
    }

private:
    NYql::NFS::IDownloaderPtr Http_;
};

}

NYql::NFS::IDownloaderPtr MakeFmrDownloader(const TFileStorageConfig& config) {
    return MakeIntrusive<TFmrDownloader>(config);
}

}
